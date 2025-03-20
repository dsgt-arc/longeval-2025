import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Memory Leak Issue:
#
# A memory leak occurs during the conversion of Spark DataFrames to Pandas
# DataFrames using toPandas(). The error message indicates that memory is not
# being released by the Arrow allocator during the toArrowBatchIterator process.
#
# Error Details:
#   - java.lang.IllegalStateException: Memory was leaked by query.
#   - Memory leaked: (327680) (or a similar size)
#   - Allocator(toArrowBatchIterator)
#   - org.apache.arrow.memory.BaseAllocator.close()
#
# Potential Causes:
#   - PyArrow version incompatibility with Spark.
#   - Spark version incompatibility with Java version.
#   - Incorrect direct memory settings for Spark.
#   - Underlying bug in Arrow or Spark related to memory management.
#   - Data size or complexity triggering the leak.


def get_spark(
    cores=os.environ.get("PYSPARK_DRIVER_CORES", os.cpu_count()),
    memory=os.environ.get("PYSPARK_DRIVER_MEMORY", "14g"),
    local_dir=os.environ.get("SPARK_LOCAL_DIR", os.environ.get("TMPDIR", "/tmp")),
    app_name="longeval",
    **kwargs,
):
    """Get a spark session for a single driver."""
    local_dir = f"{local_dir}/{int(time.time())}"
    Path(local_dir).mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.config("spark.driver.memory", memory)
        # memory leak in arrow somewhere
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.driver.maxResultSize", "8g")
        .config("spark.local.dir", local_dir)
    )
    for k, v in kwargs.items():
        builder = builder.config(k, v)
    return builder.appName(app_name).master(f"local[{cores}]").getOrCreate()


@contextmanager
def spark_resource(*args, **kwargs):
    """A context manager for a spark session."""
    spark = None
    try:
        spark = get_spark(*args, **kwargs)
        yield spark
    finally:
        if spark is not None:
            spark.stop()
