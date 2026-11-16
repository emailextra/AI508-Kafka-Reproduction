"""Check the allocation model independently and export the six-broker sweep."""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "results"


def allocate(policy, replicas, consumers, budget):
    candidates = range(replicas, budget + 1)
    if policy == "BroMax":
        candidates = reversed(candidates)
    for brokers in candidates:
        ceiling = min(30000 * brokers // replicas, 200 * brokers // (5 * replicas),
                      2000 * brokers // 25)
        if ceiling >= max(consumers, 10, 5):
            return ceiling, brokers
    return -1, -1


def main():
    with (ROOT / "calibration.csv").open(newline="") as source:
        originals = list(csv.DictReader(source))
    assert len(originals) == 84
    rows = []
    for original in originals:
        policy = original["algorithm"]
        replicas, consumers = (int(original[key]) for key in ("replicas", "consumers"))
        assert allocate(policy, replicas, consumers, 16) == (
            int(original["partitions"]), int(original["brokers"]))
        partitions, brokers = allocate(policy, replicas, consumers, 6)
        rows.append((policy, replicas, consumers, partitions, brokers, partitions >= 0))
    with (ROOT / "b6-calibration.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("algorithm", "replicas", "consumers", "partitions", "brokers", "feasible"))
        writer.writerows(rows)
    print("Matched all 84 original allocations.")
    for replicas in (3, 5):
        print(replicas, max(row[2] for row in rows if row[1] == replicas and row[-1]))


if __name__ == "__main__":
    main()
