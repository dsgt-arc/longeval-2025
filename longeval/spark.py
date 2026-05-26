import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Pick up project defaults (Spark memory, scratch dir) without requiring
# direnv. Searches upward from cwd for the nearest .env. Existing env vars
# take precedence so a shell export or .env.local override still wins.
load_dotenv()
load_dotenv(".env.local", override=True)

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
        # Disable Arrow to avoid memory leak
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.driver.maxResultSize", "8g")
        .config("spark.local.dir", local_dir)
        # Disable Hive to prevent classpath errors
        .config("spark.sql.catalogImplementation", "in-memory")
        # The LDA pipeline materializes a fat array<string> column
        # (tokens_phrased): at full-corpus scale a single 4096-row
        # vectorized-reader batch needs a >32MB contiguous buffer and
        # OOMs regardless of total heap or core count. Row-wise reads
        # have no such batch allocation; the pipeline is IO/shuffle-
        # bound so the throughput cost is negligible.
        .config("spark.sql.parquet.enableVectorizedReader", "false")
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
