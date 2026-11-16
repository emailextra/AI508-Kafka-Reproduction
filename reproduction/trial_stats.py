"""Calculate descriptive sensitivity checks from the saved trials and traces."""
import csv
import gzip
import json
from pathlib import Path
from statistics import median, quantiles

ROOT = Path(__file__).resolve().parents[1] / "results"


def read_csv(path):
    with path.open(newline="") as source:
        return list(csv.DictReader(source))


def read_latency(row):
    with gzip.open(ROOT / "live" / row["trace"], "rt", newline="") as source:
        records = [(int(r["sent_ns"]), int(r["observed_ns"])) for r in csv.DictReader(source)]
    warmup_end = min(sent for sent, _ in records) + 1_500_000_000
    return [(observed - sent) / 1e6 for sent, observed in records if sent >= warmup_end]


def p95(values):
    return quantiles(values, n=100, method="inclusive")[94]


def main():
    archive = {(row["algorithm"], row["consumers"], row["replicas"]): float(row["mean_fetch_mib_s"])
               for row in read_csv(ROOT / "published/throughput-summary.csv")
               if row["payload_bytes"] == "102400"}
    ratios = [value / archive[algorithm, consumers, "5"]
              for (algorithm, consumers, replicas), value in archive.items() if replicas == "3"]
    trials = read_csv(ROOT / "runs-003.csv")
    result = {"archive_100k_ratio": [min(ratios), max(ratios)], "latency_15": {}}
    for policy in ("BroMin", "BroMax"):
        runs = [row for row in trials if row["algorithm"] == policy and row["producers"] == "15"]
        runs.sort(key=lambda row: float(row["p95_ms"]))
        assert len(runs) == 3
        samples = [read_latency(row) for row in runs]
        for row, values in zip(runs, samples):
            assert abs(p95(values) - float(row["p95_ms"])) < 0.000001
        result["latency_15"][policy] = {
            "trial_p95_ms": [float(r["p95_ms"]) for r in runs],
            "host_cpu_percent": [float(r["cpu_percent"]) for r in runs],
            "median_trial_p95_ms": median(float(r["p95_ms"]) for r in runs),
            "pooled_p95_ms": p95(sum(samples, [])),
            "without_highest_p95_trial_ms": p95(sum(samples[:2], [])),
        }
    (ROOT / "trial-sensitivity.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
