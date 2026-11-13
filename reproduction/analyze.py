from collections import defaultdict
from pathlib import Path
import argparse
import csv
import gzip
import json
import statistics
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / ".runtime" / "analysis"))
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

COLORS = {("BroMin", 3): "#05656E", ("BroMax", 3): "#2C63A3",
          ("BroMin", 5): "#9B491D", ("BroMax", 5): "#822C64"}
plt.rcParams.update({"font.size": 10, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.grid": True,
                     "grid.alpha": .22, "axes.axisbelow": True})


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def calibration(results, figures):
    rows = []
    figure, axes = plt.subplots(1, 2, figsize=(9, 3.4), layout="constrained")
    for (algorithm, replicas), color in COLORS.items():
        data = np.loadtxt(results / f"calibration-{replicas}-16-{algorithm}.dat",
                          usecols=(3, 4, 5), dtype=int)
        label = f"{algorithm}, r = {replicas}"
        for c, partitions, brokers in data:
            rows.append(dict(algorithm=algorithm, replicas=replicas, consumers=c,
                             partitions=partitions, brokers=brokers))
            assert brokers >= replicas and brokers <= 16 and partitions >= c
            assert partitions * replicas * .005 <= brokers * .2
        for axis, values in zip(axes, (data[:, 1], data[:, 2])):
            axis.plot(data[:, 0], values, label=label, color=color,
                      linestyle="--" if algorithm == "BroMax" else "-")
            axis.set_xlabel("Consumers")
    axes[0].set_ylabel("Topic partitions")
    axes[1].set_ylabel("Brokers used")
    axes[0].legend(loc="upper left", frameon=False, fontsize=8, ncol=2)
    figure.savefig(figures / "calibration.png", dpi=200)
    plt.close(figure)
    assert len(rows) == 84
    write_csv(results / "calibration.csv", rows)


def published(results):
    source = results / "published"
    consumption = []
    delivery = []
    with tarfile.open(source / "experiment-001.tgz", "r:gz") as archive:
        for member in archive.getmembers():
            _, experiment, replicas, budget, consumers, size, policy = Path(member.name).stem.split("-")
            assert experiment == "001"
            with archive.extractfile(member) as binary:
                records = list(csv.reader(line.decode("ascii") for line in binary))
            fetch_rates = [float(record[8]) for record in records if len(record) == 10]
            consumption.append({
                "payload_bytes": int(size), "replicas": int(replicas), "algorithm": policy,
                "broker_budget": int(budget), "consumers": int(consumers),
                "samples": len(fetch_rates), "mean_fetch_mib_s": statistics.mean(fetch_rates),
            })
    with tarfile.open(source / "experiment-003.tgz", "r:gz") as archive:
        for member in archive.getmembers():
            _, experiment, replicas, producers, policy = Path(member.name).stem.split("-")
            assert experiment == "003" and replicas == "3"
            with archive.extractfile(member) as binary:
                values = [float(line.split()[1]) for line in binary if line.strip()]
            p50, p95, p99 = np.quantile(values, [.50, .95, .99])
            delivery.append({
                "producers": int(producers), "algorithm": policy, "samples": len(values),
                "p50_ms": p50, "p95_ms": p95, "p99_ms": p99,
            })
    assert len(consumption) == 48 and len(delivery) == 6
    write_csv(source / "throughput-summary.csv", consumption)
    write_csv(source / "latency-summary.csv", delivery)


def read_trials(folder):
    trials = defaultdict(list)
    latency_samples = defaultdict(list)
    raw = [json.loads(line) for line in (folder / "runs.jsonl").read_text().splitlines()]
    for trial in raw:
        if "error" in trial:
            continue
        key = (trial["experiment"], trial["algorithm"], trial["replicas"],
               trial["consumers"], trial["size"], trial["producers"])
        with gzip.open(folder / trial["trace"], "rt", newline="") as source:
            rows = list(csv.DictReader(source))
        total = sum(int(row["bytes"]) for row in rows)
        identities = {(row["partition"], row["offset"]) for row in rows}
        assert total == trial["bytes"]
        assert len(identities) == len(rows) == trial["records"]
        trial["observed_consumers"] = len({row["consumer"] for row in rows})
        if trial["experiment"] == "001":
            assert len(rows) == trial["produced"] // 2
            assert all(int(row["bytes"]) == trial["size"] for row in rows)
            trial["mib_per_s"] = total / 2**20 / trial["seconds"]
            trial["records_per_s"] = len(rows) / trial["seconds"]
            trial["mib_per_s_per_consumer"] = trial["mib_per_s"] / trial["consumers"]
        else:
            assert len(rows) == trial["produced"]
            sent = np.array([int(row["sent_ns"]) for row in rows], dtype=np.int64)
            observed = np.array([int(row["observed_ns"]) for row in rows], dtype=np.int64)
            assert np.all(observed >= sent)
            keep = sent >= sent.min() + 1_500_000_000
            latency = (observed[keep] - sent[keep]) / 1e6
            assert len(latency) > 0
            trial["latency_n"] = len(latency)
            trial["p50_ms"], trial["p95_ms"], trial["p99_ms"] = np.quantile(latency, [.50, .95, .99])
            latency_samples[key].append(latency)
        trials[key].append(trial)
    return trials, latency_samples


def throughput(trials, figures):
    rows = []
    figure, axes = plt.subplots(1, 2, figsize=(9, 3.7), layout="constrained")
    for axis, size in zip(axes, (1024, 102400)):
        axis.set_title(f"{size // 1024} KiB payloads")
        axis.set_xlabel("Consumers")
        axis.set_ylabel("Group throughput (MiB/s)")
        axis.set_xticks((5, 10, 15))
        for (algorithm, replicas), color in COLORS.items():
            scenario_rows = []
            for consumers in (5, 10, 15):
                group = trials["001", algorithm, replicas, consumers, size, 0]
                assert sorted(t["repeat"] for t in group) == [1, 2, 3]
                values = [trial["mib_per_s"] for trial in group]
                scenario_rows.append({
                    "payload_bytes": size, "replicas": replicas, "algorithm": algorithm,
                    "consumers": consumers, "partitions": group[0]["partitions"],
                    "brokers": group[0]["brokers"], "runs": len(group),
                    "mean_mib_s": statistics.mean(values), "sd_mib_s": statistics.stdev(values),
                    "mean_mib_s_per_consumer": statistics.mean(values) / consumers,
                    "mean_records_s": statistics.mean(t["records_per_s"] for t in group),
                    "mean_host_cpu_percent": statistics.mean(t["cpu_percent"] for t in group),
                    "min_observed_consumers": min(t["observed_consumers"] for t in group),
                    "trials_with_idle_consumers": sum(t["observed_consumers"] < consumers for t in group),
                })
            rows.extend(scenario_rows)
            axis.errorbar([row["consumers"] for row in scenario_rows],
                          [row["mean_mib_s"] for row in scenario_rows],
                          yerr=[row["sd_mib_s"] for row in scenario_rows],
                          color=color, label=f"{algorithm}, r = {replicas}",
                          marker="o", capsize=3, linestyle="--" if algorithm == "BroMax" else "-")
    axes[1].legend(frameon=False, fontsize=8)
    figure.savefig(figures / "throughput.png", dpi=200)
    plt.close(figure)
    return rows


def latency(trials, samples, figures):
    rows = []
    figure, axes = plt.subplots(1, 3, figsize=(9, 3.55), sharex=True, sharey=True, layout="constrained")
    archived = {}
    with tarfile.open(ROOT / "results/published/experiment-003.tgz", "r:gz") as archive:
        for member in archive.getmembers():
            _, _, _, producers, algorithm = Path(member.name).stem.split("-")
            with archive.extractfile(member) as source:
                archived[int(producers), algorithm] = np.sort(
                    [float(line.split()[1]) for line in source if line.strip()])
    for axis, producers in zip(axes, (10, 15, 20)):
        axis.set_title(f"{producers} producers")
        axis.set_xlabel("Latency (ms)")
        axis.set_xscale("symlog", linthresh=1)
        for algorithm in ("BroMin", "BroMax"):
            key = ("003", algorithm, 3, 5, 0, producers)
            group = trials[key]
            assert sorted(t["repeat"] for t in group) == [1, 2, 3]
            values = np.sort(np.concatenate(samples[key]))
            p50, p95, p99 = np.quantile(values, [.50, .95, .99])
            rows.append({
                "producers": producers, "algorithm": algorithm,
                "brokers": group[0]["brokers"], "partitions": group[0]["partitions"],
                "runs": len(group), "samples": len(values), "p50_ms": p50,
                "p95_ms": p95, "p99_ms": p99,
                "median_run_p95_ms": statistics.median(t["p95_ms"] for t in group),
                "min_run_p95_ms": min(t["p95_ms"] for t in group),
                "max_run_p95_ms": max(t["p95_ms"] for t in group),
                "mean_host_cpu_percent": statistics.mean(t["cpu_percent"] for t in group),
            })
            axis.plot(values, np.arange(1, len(values) + 1) / len(values),
                      label=f"{algorithm} local", color=COLORS[algorithm, 3])
            original = archived[producers, algorithm]
            axis.plot(original, np.arange(1, len(original) + 1) / len(original),
                      label=f"{algorithm} archive", color=COLORS[algorithm, 3], linestyle="--")
        axis.set_ylim(0, 1.025)
    axes[0].set_ylabel("Empirical cumulative fraction")
    axes[0].legend(frameon=False, loc="lower right", fontsize=8)
    figure.savefig(figures / "latency.png", dpi=200)
    plt.close(figure)
    return rows


def throughput_comparison(local, figures):
    with (ROOT / "results/published/throughput-summary.csv").open(newline="") as source:
        archive = list(csv.DictReader(source))
    figure, axes = plt.subplots(2, 2, figsize=(9, 6), sharey="row", layout="constrained")
    for row, size in zip(axes, (1024, 102400)):
        for (algorithm, replicas), color in COLORS.items():
            observed = sorted((point for point in local if
                               (point["payload_bytes"], point["algorithm"], point["replicas"])
                               == (size, algorithm, replicas)), key=lambda point: point["consumers"])
            original = sorted((point for point in archive if
                               (int(point["payload_bytes"]), point["algorithm"], int(point["replicas"]))
                               == (size, algorithm, replicas)), key=lambda point: int(point["consumers"]))
            row[0].plot([point["consumers"] for point in observed],
                        [point["mean_mib_s_per_consumer"] for point in observed],
                        marker="o", color=color, label=f"{algorithm}, r = {replicas}")
            row[1].plot([int(point["consumers"]) for point in original],
                        [float(point["mean_fetch_mib_s"]) for point in original],
                        marker="o", color=color)
        row[0].set_title(f"Local group rate ÷ consumers · {size // 1024} KiB")
        row[1].set_title(f"Authors’ mean client fetch rate · {size // 1024} KiB")
        row[0].set_xticks([5, 10, 15])
        row[1].set_xticks([25, 45, 65, 85, 105, 125])
        row[0].set_ylabel("MiB/s per consumer")
        for axis in row:
            axis.set_xlabel("Configured consumers")
    axes[0, 0].legend(fontsize=8, frameon=False)
    figure.savefig(figures / "throughput-comparison.png", dpi=200)
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="live")
    parser.add_argument("--calibration-only", action="store_true")
    parser.add_argument("--published-only", action="store_true")
    parser.add_argument("--throughput-run", help="Use Experiment 001 trials from this run folder")
    args = parser.parse_args()
    results = ROOT / "results"
    figures = results / "figures"
    figures.mkdir(exist_ok=True)
    if args.published_only:
        published(results)
        return
    calibration(results, figures)
    if args.calibration_only:
        return
    published(results)
    trials, samples = read_trials(results / args.name)
    if args.throughput_run:
        rerun, _ = read_trials(results / args.throughput_run)
        assert {key for key in rerun} == {key for key in trials if key[0] == "001"}
        trials.update(rerun)
    assert len(trials) == 30 and sum(map(len, trials.values())) == 90
    throughput_rows = throughput(trials, figures)
    throughput_comparison(throughput_rows, figures)
    latency_rows = latency(trials, samples, figures)
    write_csv(results / "throughput-summary.csv", throughput_rows)
    write_csv(results / "latency-summary.csv", latency_rows)
    raw_rows = [trial for group in trials.values() for trial in group]
    for experiment in ("001", "003"):
        write_csv(results / f"runs-{experiment}.csv",
                  [row for row in raw_rows if row["experiment"] == experiment])
    print(f"Validated {len(raw_rows)} trials and wrote {len(throughput_rows) + len(latency_rows)} summaries.")


if __name__ == "__main__":
    main()
