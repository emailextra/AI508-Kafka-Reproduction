# AI508 Kafka reproduction

The report covers the numerical allocation in Figure 6 and scaled local runs of Experiments 001 and 003. The upstream reference commit is `19e9874c9414ceb1c036f856f1297a84e82ea4f3`. The report describes the differences from the published setup and measures.

## Files

| Path | Contents |
| --- | --- |
| `reproduction/setup.ps1` | Windows setup for Java, Kafka, Python dependencies, and the upstream clone |
| `reproduction/benchmark.py` | Starts local brokers, runs the matrix, and records individual observations |
| `reproduction/analyze.py` | Validates all traces, calculates scenario summaries, and plots results |
| `results/calibration-*.dat`, `results/calibration.csv` | Author-script output and consolidated allocations |
| `results/allocation-tests.txt` | Log of the six upstream allocation tests |
| `results/throughput-summary.csv`, `results/latency-summary.csv` | Per-scenario means, spread, and quantiles |
| `results/runs-001.csv`, `results/runs-003.csv` | Per-trial metrics |
| `results/live/` | Environment, run manifest, compressed per-record traces, and broker configurations |
| `results/pilot/` | Replication-factor-five smoke-test evidence |
| `results/figures/` | Calibration, consumption throughput, and latency figures |
| `results/published/` | Authors’ raw archives and regenerated reference summaries |

## Repeat the measurements

Use Windows PowerShell and 64-bit Python 3.12 with pip. Java, Kafka, and the other packages install inside `.runtime`. The local ports are 22181 for ZooKeeper and 19091 through 19096 for brokers. Run from the assignment root.

```powershell
$python = (Get-Command python.exe).Source
./reproduction/setup.ps1 -Python $python
& $python -m unittest discover -s upstream/allocation -v
New-Item -ItemType Directory -Force results | Out-Null
Push-Location results
& $python ../upstream/allocation/calibration.py
Pop-Location
& $python reproduction/benchmark.py --pilot --name pilot-repeat
& $python reproduction/benchmark.py --name live-repeat
& $python reproduction/analyze.py --name live-repeat
```

Use a fresh run name for each independent benchmark. To continue after a stopped run, use its original name with `--resume`. The runner retains the broker data under `.runtime/cluster-data/`, which can use several gigabytes. Kafka brokers stop when the batch exits. Analysis requires all 90 successful trials and retains any failed attempt in the live manifest. Reanalysis updates the figures and summaries with the run selected by `--name`. The report describes the submitted run.

## Measures

Experiment 001 uses aggregate unique payload bytes divided by the group wall time. Group rates are reported in MiB/s. Trial-level standard deviations describe variability across repeats. Experiment 003 computes latency from the first eight payload bytes, an unsigned nanosecond timestamp, to the time the consumer receives the record. The plotted distribution pools three runs after excluding records sent in the first 1.5 seconds of each run. The raw trace preserves that interval. CPU, memory, and disk counters are host-wide. In the trace, `partition` and `offset` identify a record within its topic, and `consumer` identifies the worker that received it.

The upstream repository is MIT licensed. The original paper is cited in the report. Local experiment outputs were produced by the scripts in this package. The published archives were downloaded from the URLs linked in the repository’s experiment READMEs. Their SHA-256 hashes can be used to check file integrity.

## Benchmark revisions

The archived short-window trials are in results/live. The three-broker batch and original five-broker batch used the default Kafka index-size setting. The resumed five-broker and six-broker batches used log.index.size.max.bytes=65536. The current runner uses 65536 for all batches, so a fresh run does not exactly recreate the original three-broker batch. The archived properties files record each actual configuration.

Run the larger-preload Experiment 001 matrix with:

```powershell
& $python reproduction/benchmark.py --long-throughput --name throughput-long
& $python reproduction/analyze.py --throughput-run throughput-long
```

Use the second command once all 72 longer-preload trials have completed. Experiment 003 continues to use the original latency traces.

The packaged results/throughput-long folder contains 7 completed follow-up trials and the recorded preload failure. These partial results are diagnostic and are excluded from the main comparisons.
