# Databricks notebook source
# MAGIC %md
# MAGIC # Spike pipeline: SCD2 via AUTO CDC
# MAGIC
# MAGIC Run this as a **Lakeflow pipeline** source file, not as a notebook.
# MAGIC Default catalog `bosch_poc`, default schema `spike`, serverless, triggered mode.

# COMMAND ----------

try:
    from pyspark import pipelines as dp
except ImportError:
    # Older runtimes expose the same functions from the dlt module.
    import dlt as dp

from pyspark.sql.functions import col, expr

SOURCE_TABLE = spark.conf.get("spike.source_table", "bosch_poc.spike.bronze_change_feed")
TARGET = "entity_history"

# COMMAND ----------


@dp.view(name="change_feed")
def change_feed():
    return spark.readStream.table(SOURCE_TABLE)


# COMMAND ----------

dp.create_streaming_table(TARGET)

# Everything assign_versions() does by hand -- collapsing no-op resends, closing the previous
# version, ordering full and delta events into one chain -- is this call.
dp.create_auto_cdc_flow(
    target=TARGET,
    source="change_feed",
    keys=["entity_id"],
    sequence_by=col("seq"),
    apply_as_deletes=expr("op = 'DELETE'"),
    except_column_list=["op", "seq"],
    stored_as_scd_type=2,
)
