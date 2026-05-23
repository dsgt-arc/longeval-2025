"""Free conservative re-merge sweep for query expansion (no new API calls).

The generated expansion files store {original, terms}; ORIGINAL_WEIGHT and the
term count are applied at merge time, so we can re-merge any (n_terms, weight)
from the stored terms and re-score on one date in a single Spark session.

n_terms=0 is a sanity anchor: query = original repeated `weight`x, which is a
monotonic scaling of the baseline query -> identical BM25 ranking -> must equal
the baseline NDCG@10 (0.3127 for 2023-01) regardless of weight.
"""
import glob
import json
import os
import sys

from pyspark.sql import functions as F

from longeval.experiment.bm25.retrieval import run_search
from longeval.experiment.bm25.evaluation import score_search
from longeval.spark import get_spark

DATE = "2023-01"
TRAIN_ROOT = (
    "/mnt/data/scratch/longeval/longeval-web/release_2025_p1/"
    "release_2025_p1/French/LongEval Train Collection"
)
INDEX_PATH = f"/mnt/data/tmp/longeval-bm25-validate/index_trec/date={DATE}"
EXP_ROOT = os.path.expanduser(
    f"~/scratch/longeval/query_expansion/{sys.argv[1] if len(sys.argv) > 1 else 'french'}"
)
GRID_TERMS = [0, 3, 5, 8, 12]
GRID_WEIGHT = [2, 3, 4]

spark = get_spark()

raw = (
    spark.read.csv(
        f"{TRAIN_ROOT}/queries/{DATE}_queries.txt",
        sep="\t",
        schema="qid STRING, query STRING",
    )
    .where(F.col("query").isNotNull())
    .toPandas()
)

# {qid: terms} from the generated expansion; qids missing (refused) fall back to
# the original text (empty terms), matching the scorer's coalesce behavior.
exp = {}
for f in glob.glob(f"{EXP_ROOT}/expansion/*.json"):
    for r in json.load(open(f)):
        exp[r["qid"]] = r["terms"]
print(f"variant={EXP_ROOT.split('/')[-1]} expansion qids={len(exp)} raw queries={len(raw)}")

qrels = spark.read.csv(
    f"{TRAIN_ROOT}/qrels/{DATE}_fr/qrels_processed.txt",
    sep=" ",
    schema="qid STRING, rank INT, docid STRING, rel INT",
)


def score(n_terms, weight):
    rows = []
    for t in raw.itertuples():
        terms = exp.get(t.qid, [])[:n_terms]
        q = " ".join([*([t.query] * weight), *terms]).strip()
        rows.append((t.qid, q))
    qdf = spark.createDataFrame(rows, "qid STRING, query STRING")
    results = run_search(qdf, INDEX_PATH, k=100).withColumn(
        "docid", F.regexp_replace("docid", "^doc", "")
    )
    agg = score_search(results, qrels).agg(F.mean("ndcg_cut_10").alias("m")).collect()[0]
    return agg["m"]


print("\n==== expansion re-merge sweep (2023-01) — baseline = 0.3127 ====", file=sys.stderr)
print(f"{'n_terms':>8} {'weight':>7} {'NDCG@10':>9}", file=sys.stderr)
for nt in GRID_TERMS:
    for w in GRID_WEIGHT:
        m = score(nt, w)
        print(f"{nt:>8} {w:>7} {m:>9.4f}", file=sys.stderr)
        if nt == 0:
            break  # weight-invariant when no terms; one run suffices as anchor
