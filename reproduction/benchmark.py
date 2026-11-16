from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import argparse
import csv
import gzip
import json
import os
import platform
import random
import subprocess
import sys
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".runtime"
RESULTS = ROOT / "results"
sys.path[:0] = [str(RUNTIME / "python"), str(ROOT / "upstream" / "allocation")]

import confluent_kafka
import psutil
from algorithms import ProblemInstance
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

MODEL = ProblemInstance(T=100e6, L=.2, U=2, Tp=10e6, Tc=20e6,
                        Hmax=30000, lr=.005, u=.025)
LOGGER = "log4j.rootLogger=ERROR, stderr\nlog4j.appender.stderr=org.apache.log4j.ConsoleAppender\nlog4j.appender.stderr.Target=System.err\nlog4j.appender.stderr.layout=org.apache.log4j.SimpleLayout\n"


@contextmanager
def cluster(brokers, run_dir):
    java = next(RUNTIME.glob("jdk-*/bin/java.exe"))
    kafka = RUNTIME / "kafka_2.13-3.4.1"
    log_config = run_dir / "log4j.properties"
    log_config.write_text(LOGGER)
    args = [str(java), "-Xms128m", "-Xmx384m", "-XX:ActiveProcessorCount=2",
            f"-Dlog4j.configuration={log_config.as_uri()}", "-cp", str(kafka / "libs" / "*")]
    startup = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    processes = []
    logs = []

    def start(class_name, path, name):
        logfile = (run_dir / f"{name}.log").open("wb")
        logs.append(logfile)
        process = subprocess.Popen([*args, class_name, str(path)], stdout=logfile,
                                   stderr=subprocess.STDOUT, creationflags=startup)
        processes.append(process)

    try:
        storage = RUNTIME / "cluster-data" / run_dir.parent.name / run_dir.name
        zk = run_dir / "zookeeper.properties"
        zk.write_text(f"dataDir={(storage / 'zk').as_posix()}\nclientPort=22181\n"
                      "clientPortAddress=127.0.0.1\nadmin.enableServer=false\n")
        start("org.apache.zookeeper.server.quorum.QuorumPeerMain", zk, "zookeeper")
        for index in range(brokers):
            port = 19091 + index
            config = run_dir / f"broker-{index}.properties"
            config.write_text(
                f"broker.id={index}\nlisteners=PLAINTEXT://127.0.0.1:{port}\n"
                f"advertised.listeners=PLAINTEXT://127.0.0.1:{port}\n"
                f"log.dirs={(storage / f'data-{index}').as_posix()}\n"
                "zookeeper.connect=127.0.0.1:22181\nauto.create.topics.enable=false\n"
                "offsets.topic.replication.factor=3\noffsets.topic.num.partitions=6\n"
                "group.initial.rebalance.delay.ms=1000\nnum.partitions=1\n"
                "num.network.threads=2\nnum.io.threads=4\nnum.replica.fetchers=1\n"
                "log.index.size.max.bytes=65536\n"
            )
            start("kafka.Kafka", config, f"broker-{index}")
        bootstrap = "127.0.0.1:19091"
        admin = AdminClient({"bootstrap.servers": bootstrap, "log_level": 0})
        until = time.monotonic() + 90
        while time.monotonic() < until:
            if any(process.poll() is not None for process in processes):
                raise RuntimeError(f"A server exited during startup. Check {run_dir}")
            try:
                if len(admin.list_topics(timeout=2).brokers) == brokers:
                    yield bootstrap, admin
                    return
            except confluent_kafka.KafkaException:
                pass
            time.sleep(1)
        raise RuntimeError(f"Cluster did not register {brokers} brokers. Check {run_dir}")
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        for process in processes:
            process.wait(timeout=20)
        for logfile in logs:
            logfile.close()


def create_topic(admin, topic, partitions, replicas):
    admin.create_topics([NewTopic(topic, partitions, replicas)], request_timeout=45)[topic].result()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        metadata = admin.list_topics(topic, timeout=5).topics[topic]
        if len(metadata.partitions) == partitions and all(
            p.leader >= 0 and len(p.isrs) == replicas for p in metadata.partitions.values()
        ):
            return
        time.sleep(.25)
    raise RuntimeError(f"Topic {topic} did not reach full replication")


def send_records(bootstrap, topic, count, min_bytes, max_bytes, seed, rate=0, start=None,
                 timeout_ms=30000, linger_ms=0):
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all",
                         "linger.ms": linger_ms, "compression.type": "none",
                         "message.timeout.ms": timeout_ms})
    errors = []
    rng = random.Random(seed)
    payload = rng.randbytes(max_bytes)
    if start is not None:
        start.wait(timeout=45)
    began = time.perf_counter()
    for number in range(count):
        if rate:
            time.sleep(max(0, began + number / rate - time.perf_counter()))
        length = rng.randint(min_bytes, max_bytes)
        timestamp = time.time_ns().to_bytes(8, "big")
        value = timestamp + payload[8:length]
        while True:
            try:
                producer.produce(topic, value=value,
                                 on_delivery=lambda error, _: errors.append(str(error)) if error else None)
                break
            except BufferError:
                producer.poll(.05)
        producer.poll(0)
    if producer.flush(timeout_ms / 1000 + 10) or errors:
        raise RuntimeError(f"Incomplete production: {errors[:3]}")


def consume_group(bootstrap, topic, clients, expected, writer, feed=None):
    ready = [threading.Event() for _ in range(clients)]
    started = threading.Event()
    finished = threading.Event()
    lock = threading.Lock()
    seen = set()
    samples = []
    timing = {}
    deadline = time.monotonic() + 95

    def consume(index):
        consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": topic,
                             "auto.offset.reset": "earliest", "enable.auto.commit": False,
                             "partition.assignment.strategy": "range"})
        def on_assign(client, parts):
            if not parts:
                raise RuntimeError(f"Consumer {index} received no partitions")
            client.assign(parts)
            if not started.is_set():
                client.pause(parts)
            ready[index].set()

        consumer.subscribe([topic], on_assign=on_assign)
        try:
            while not started.is_set() and not finished.is_set():
                if time.monotonic() > deadline:
                    raise TimeoutError("Consumer group did not start")
                consumer.poll(.05)
            if not finished.is_set():
                consumer.resume(consumer.assignment())
            while not finished.is_set():
                if time.monotonic() > deadline:
                    raise TimeoutError(f"Only {len(seen)} of {expected} records arrived")
                if feed is None:
                    batch = consumer.consume(num_messages=128, timeout=.02)
                else:
                    message = consumer.poll(.02)
                    batch = [] if message is None else [message]
                observed = time.time_ns()
                with lock:
                    for msg in batch:
                        if finished.is_set():
                            break
                        if msg.error():
                            raise RuntimeError(msg.error())
                        identity = (msg.partition(), msg.offset())
                        if identity in seen:
                            raise RuntimeError(f"Duplicate offset {identity}")
                        seen.add(identity)
                        value = msg.value()
                        sent = int.from_bytes(value[:8], "big")
                        samples.append((index, *identity, len(value), sent, observed))
                        if len(seen) == expected:
                            timing["end"] = time.perf_counter()
                            finished.set()
        finally:
            consumer.close()

    with ThreadPoolExecutor(max_workers=clients + 1) as pool:
        tasks = [pool.submit(consume, i) for i in range(clients)]
        try:
            for event in ready:
                while not event.wait(.1):
                    for task in tasks:
                        if task.done():
                            task.result()
                    if time.monotonic() > deadline - 55:
                        raise TimeoutError("Group readiness timed out")
            psutil.cpu_percent()
            disk_before = psutil.disk_io_counters()
            timing["start"] = time.perf_counter()
            started.set()
            if feed is not None:
                feed()
            for task in tasks:
                task.result()
            disk_after = psutil.disk_io_counters()
            cpu = psutil.cpu_percent()
        finally:
            finished.set()
            started.set()

    writer.writerow(("consumer", "partition", "offset", "bytes", "sent_ns", "observed_ns"))
    writer.writerows(samples)
    elapsed = timing["end"] - timing["start"]
    return {
        "records": len(samples), "bytes": sum(row[3] for row in samples),
        "seconds": elapsed, "cpu_percent": cpu,
        "ram_percent": psutil.virtual_memory().percent,
        "disk_read_bytes": disk_after.read_bytes - disk_before.read_bytes if disk_after else None,
        "disk_write_bytes": disk_after.write_bytes - disk_before.write_bytes if disk_after else None,
    }


def trial(bootstrap, admin, scenario, folder, sequence, long_throughput=False):
    experiment, algo, r, clients, size, producers, partitions, brokers, repeat = scenario
    topic = f"ai508-{sequence}-{uuid.uuid4().hex[:8]}"
    create_topic(admin, topic, partitions, r)
    seed = 20261111 + repeat
    if experiment == "001":
        preloads = {1024: 250000, 102400: 20000} if long_throughput else {1024: 12000, 102400: 2400}
        records = 6000 if algo == "FixedBrokers" else preloads[size]
        send_records(bootstrap, topic, records, size, size, seed,
                     timeout_ms=120000 if algo == "FixedBrokers" else (180000 if long_throughput else 30000),
                     linger_ms=10 if algo == "FixedBrokers" else 0)
        feed = None
        expected = records // 2
    else:
        records = 450
        expected = records * producers
        def feed():
            start = threading.Barrier(producers)
            with ThreadPoolExecutor(max_workers=producers) as pool:
                jobs = [pool.submit(send_records, bootstrap, topic, records, 1024, 102400,
                                    seed + index, 30, start) for index in range(producers)]
                for job in jobs:
                    job.result()

    filename = f"{topic}.csv.gz"
    with gzip.open(folder / filename, "wt", newline="") as output:
        metrics = consume_group(bootstrap, topic, clients, expected, csv.writer(output), feed)
    metrics.update(experiment=experiment, algorithm=algo, replicas=r, consumers=clients,
                   size=size, producers=producers, partitions=partitions, brokers=brokers,
                   repeat=repeat, trace=filename, produced=records * max(1, producers))
    return metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot", action="store_true")
    parser.add_argument("--long-throughput", action="store_true")
    parser.add_argument("--fixed-brokers", action="store_true",
                        help="Compare 40 and 80 partitions on six brokers using larger preloads")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--name", default="live")
    parser.add_argument("--resume", action="store_true")
    options = parser.parse_args()
    folder = RESULTS / options.name
    folder.mkdir(parents=True, exist_ok=options.resume)
    environment = {
        "platform": platform.platform(), "python": platform.python_version(),
        "logical_cpus": psutil.cpu_count(), "physical_cpus": psutil.cpu_count(logical=False),
        "ram_bytes": psutil.virtual_memory().total, "kafka": "3.4.1",
        "confluent_kafka": confluent_kafka.version(), "java": next(RUNTIME.glob("jdk-*")).name,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "upstream_commit": "19e9874c9414ceb1c036f856f1297a84e82ea4f3"
    }
    environment_file = folder / "environment.json"
    if not environment_file.exists():
        environment_file.write_text(json.dumps(environment, indent=2))
    scenarios = []
    for rep in range(1, options.repeats + 1):
        for r in (3, 5):
            for clients in (5, 10, 15):
                for size in (1024, 102400):
                    for algo in ("BroMin", "BroMax"):
                        p, b = MODEL.solve(algo, clients, r, 6)
                        scenarios.append(("001", algo, r, clients, size, 0, p, b, rep))
        for producers in (10, 15, 20):
            for algo in ("BroMin", "BroMax"):
                p, b = MODEL.solve(algo, 5, 3, 6)
                scenarios.append(("003", algo, 3, 5, 0, producers, p, b, rep))
    if options.long_throughput:
        scenarios = [scenario for scenario in scenarios if scenario[0] == "001"]
    if options.fixed_brokers:
        scenarios = [("001", "FixedBrokers", 3, 10, 102400, 0, partitions, 6, repeat)
                     for repeat in range(1, options.repeats + 1) for partitions in (40, 80)]
    if options.pilot:
        scenarios = [("001", "BroMin", 5, 5, 1024, 0, 40, 5, 1)]
    order = random.Random(508)
    order.shuffle(scenarios)
    run_log = folder / "runs.jsonl"
    completed = set()
    if options.resume and run_log.exists():
        for line in run_log.read_text().splitlines():
            entry = json.loads(line)
            if "error" not in entry:
                completed.add(tuple(entry[key] for key in (
                    "experiment", "algorithm", "replicas", "consumers", "size",
                    "producers", "partitions", "brokers", "repeat"
                )))
    with run_log.open("a", buffering=1) as output:
        for brokers in sorted({scenario[7] for scenario in scenarios}):
            pending = [(i, scenario) for i, scenario in enumerate(scenarios, start=1)
                       if scenario[7] == brokers and scenario not in completed]
            if not pending:
                continue
            suffix = f"-resume-{uuid.uuid4().hex[:6]}" if options.resume else ""
            broker_folder = folder / f"cluster-{brokers}{suffix}"
            broker_folder.mkdir()
            with cluster(brokers, broker_folder) as (bootstrap, admin):
                for sequence, scenario in pending:
                    print(f"Starting {sequence}/{len(scenarios)} {scenario}", flush=True)
                    try:
                        result = trial(bootstrap, admin, scenario, folder, sequence,
                                       options.long_throughput or options.fixed_brokers)
                    except Exception as exc:
                        result = {"scenario": scenario, "error": repr(exc)}
                        output.write(json.dumps(result) + "\n")
                        raise
                    output.write(json.dumps(result) + "\n")
                    print(f"Completed {sequence}: {result['records']} records", flush=True)


if __name__ == "__main__":
    main()
