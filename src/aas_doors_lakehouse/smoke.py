"""Smoke job used to verify the toolchain end to end.

Proves that the wheel is installed, that the console script resolves, and that a
Databricks Connect session can be created. Serves as the reference shape for
real jobs: pure, testable functions plus a thin ``main`` entry point.
"""

from typing import TYPE_CHECKING

from databricks.connect import DatabricksSession

from aas_doors_lakehouse import __version__

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SAMPLE_ROWS = [(1, "alpha"), (2, "beta"), (3, "gamma")]
SAMPLE_SCHEMA = "id INT, name STRING"


def build_banner() -> str:
    """Return a one-line identity banner for the running wheel."""
    return f"aas-doors-lakehouse {__version__}"


def run(spark: "SparkSession") -> int:
    """Materialise a small DataFrame on the cluster and return its row count."""
    df = spark.createDataFrame(SAMPLE_ROWS, SAMPLE_SCHEMA)
    df.show()
    return df.count()


def main() -> None:
    """Console script and Asset Bundle wheel-task entry point."""
    print(build_banner())
    print(f"rows: {run(DatabricksSession.builder.getOrCreate())}")
