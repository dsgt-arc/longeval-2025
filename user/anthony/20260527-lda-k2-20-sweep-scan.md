# LDA K=2..20 sweep scan

Date: 2026-05-27

## Context

Issue #36 requested extending the LDA K-selection sweep to a single comparable curve over every integer K from 2 through 20.

A draft PR was opened with the code changes:

- PR: https://github.com/dsgt-arc/longeval-2025/pull/39
- Branch: `issue-36-lda-k-sweep`
- Commit: `dc6823c Extend LDA K sweep to 2..20`

## Code changes

- Default `lda-sweep` K grid changed to `2,3,4,...,20`.
- Default `lda-topic-proportions` K grid changed to match.
- `TrainLDASweep` now checks per-K model + `_train_config.json` stamp and only fits missing/stale K values.
- `InferLDASweep` now checks per-K `docTopicDistribution_lda.parquet` + `_inference_config.json` stamp and only scores missing/stale K values.
- Regression tests added for missing-K detection.

Validation before the long run:

```text
uv run ruff check longeval/etl/lda/workflow.py tests/lda_tests/test_workflow_sweep.py
uv run pytest tests/lda_tests/test_workflow_sweep.py -q
# 21 passed
```

## Runtime setup

To avoid mutating the existing K=2..10 sweep root, a fresh root was created:

```text
/mnt/data/tmp/lda-k2-20-sweep
```

It was seeded by hardlinking from:

```text
/mnt/data/tmp/lda-k2-10-sweep
```

Seeded items:

```text
preprocess/
k2/ ... k10/
```

Input corpus:

```text
/mnt/data/tmp/longeval-train-parquet
```

Important parameters used to preserve comparability with the K=2..10 run:

```text
--date all
--sample-fraction 1.0
```

Preflight status before launch:

```text
mine phrases complete True
build features complete True
train missing [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
infer missing [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
aggregate sweep complete False
aggregate topic proportions complete False
```

Launch command:

```bash
PYSPARK_DRIVER_CORES=8 \
PYSPARK_DRIVER_MEMORY=36g \
SPARK_LOCAL_IP=127.0.0.1 \
uv run longeval etl lda-sweep \
  "2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20" \
  /mnt/data/tmp/longeval-train-parquet \
  /mnt/data/tmp/lda-k2-20-sweep \
  --date all \
  --sample-fraction 1.0
```

Log:

```text
artifacts/logs/lda-sweep-k2-20-20260526-232443.log
```

Exit status:

```text
0
```

Completion checks after the run:

```text
AggregateLDASweep.complete True
AggregateTopicProportions.complete True
```

Root outputs:

```text
/mnt/data/tmp/lda-k2-20-sweep/sweep_coherence.parquet
/mnt/data/tmp/lda-k2-20-sweep/topic_proportions.parquet
/mnt/data/tmp/lda-k2-20-sweep/topic_drift.parquet
```

## K-selection results

Metric used here:

```text
coherence_diversity = mean_npmi * topic_diversity
```

Full curve:

| K | mean NPMI | topic diversity | coherence diversity | n docs |
|---:|---:|---:|---:|---:|
| 2 | 0.244944 | 1.000000 | 0.244944 | 16,263,471 |
| 3 | 0.329847 | 1.000000 | 0.329847 | 16,263,471 |
| 4 | 0.350851 | 1.000000 | 0.350851 | 16,263,471 |
| 5 | 0.316523 | 0.980000 | 0.310193 | 16,263,471 |
| 6 | 0.224074 | 0.966667 | 0.216604 | 16,263,471 |
| 7 | 0.211098 | 0.971429 | 0.205066 | 16,263,471 |
| 8 | 0.232861 | 0.962500 | 0.224128 | 16,263,471 |
| 9 | 0.273287 | 0.955556 | 0.261141 | 16,263,471 |
| 10 | 0.271099 | 0.950000 | 0.257544 | 16,263,471 |
| 11 | 0.239178 | 0.927273 | 0.221784 | 16,263,471 |
| 12 | 0.256344 | 0.916667 | 0.234982 | 16,263,471 |
| 13 | 0.341019 | 0.930769 | 0.317410 | 16,263,471 |
| 14 | 0.339860 | 0.914286 | 0.310729 | 16,263,471 |
| 15 | 0.319553 | 0.940000 | 0.300380 | 16,263,471 |
| 16 | 0.318102 | 0.956250 | 0.304185 | 16,263,471 |
| 17 | 0.297645 | 0.941176 | 0.280137 | 16,263,471 |
| 18 | 0.278847 | 0.938889 | 0.261807 | 16,263,471 |
| 19 | 0.355041 | 0.926316 | 0.328880 | 16,263,471 |
| 20 | 0.347640 | 0.930000 | 0.323305 | 16,263,471 |

Best K by `coherence_diversity`:

```text
K=4, coherence_diversity=0.350851
```

Best new points above K=10:

```text
K=19: coherence_diversity=0.328880
K=20: coherence_diversity=0.323305
K=13: coherence_diversity=0.317410
```

Conclusion: extending the comparable curve to K=2..20 did not change the winner under the coherence-diversity metric. K=4 remains the best choice.

## Notes on coherence

The coherence metric is doc-level NPMI over topic top words. A topic scores higher when its top words co-occur in documents more often than chance. `topic_diversity` penalizes duplicated/reused top words across topics. `coherence_diversity` combines these into one K-selection score.

