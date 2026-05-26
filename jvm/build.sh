#!/usr/bin/env bash
# Build the native Lucene-FR analyzer UDF jar.
#
# Pure javac + jar — deliberately no sbt/Maven so the only build infra is
# the JDK already pinned in .env (JAVA_HOME). Reproducible: classpath is
# the anserini fatjar pyserini ships (provides anserini + its Lucene) and
# Spark's UDF interface jar, both resolved from .venv.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"

# JAVA_HOME comes from .env (loaded by direnv) or the environment.
if [[ -z "${JAVA_HOME:-}" ]]; then
  jh="$(grep -E '^JAVA_HOME=' "$root/.env" 2>/dev/null | head -1 | cut -d= -f2-)"
  [[ -n "$jh" ]] || { echo "JAVA_HOME unset and not in .env" >&2; exit 1; }
  JAVA_HOME="$jh"
fi

sp="$root/.venv/lib"/python*/site-packages
fatjar="$(ls $sp/pyserini/resources/jars/anserini-*-fatjar.jar | head -1)"
sqlapi="$(ls $sp/pyspark/jars/spark-sql-api_*.jar | head -1)"
[[ -f "$fatjar" ]] || { echo "anserini fatjar not found under $sp" >&2; exit 1; }
[[ -f "$sqlapi" ]] || { echo "spark-sql-api jar not found under $sp" >&2; exit 1; }

out="$here/build"
rm -rf "$out"
mkdir -p "$out"

"$JAVA_HOME/bin/javac" --release 17 -proc:none -cp "$fatjar:$sqlapi" -d "$out" \
  "$here/src/com/longeval/LuceneFrAnalyzerUDF.java"
"$JAVA_HOME/bin/jar" cf "$here/longeval-analyzer.jar" -C "$out" .

echo "built $here/longeval-analyzer.jar (against $(basename "$fatjar"))"
