#!/usr/bin/env bash
set -ex

spark_dir=$(python -c 'import site; print(site.getsitepackages()[0])')
jar_dir="${spark_dir}/pyspark/jars"
mvn dependency:copy-dependencies
# copy just the single jar we need
cp target/dependency/opensearch* ${jar_dir}
