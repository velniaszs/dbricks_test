"""Proves the workspace isolation fixture before anything relies on it."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from bedi_lakehouse.naming import LAYERS, Stream

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from bedi_lakehouse.naming import Layout

pytestmark = pytest.mark.workspace


def test_every_layer_schema_exists(spark: SparkSession, layout: Layout) -> None:
    schemas = {row[0] for row in spark.sql(f"SHOW SCHEMAS IN {layout.catalog}").collect()}
    assert {layout.schema(layer) for layer in LAYERS} <= schemas


def test_the_suffix_is_unique_to_this_run(layout: Layout) -> None:
    assert layout.schema_suffix.startswith("_test_")
    assert layout.schema("bronze") != "bronze"


def test_a_table_can_be_written_and_read_back(spark: SparkSession, layout: Layout) -> None:
    table = layout.bronze(Stream("aas_doors", "requirement"))
    spark.range(3).write.saveAsTable(table)
    assert spark.table(table).count() == 3
