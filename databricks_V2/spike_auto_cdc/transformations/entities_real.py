# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2 pipeline: real entities via AUTO CDC
# MAGIC
# MAGIC Lakeflow pipeline source file. Default catalog `bosch_poc`, schema `spike`, serverless,
# MAGIC triggered. Run `explorations/10_stage2_change_feed.py` first.
# MAGIC
# MAGIC The feed is rebuilt with `overwrite`, so use **Full refresh all** on every run.

# COMMAND ----------

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

from pyspark.sql.functions import col, expr

SOURCE_TABLE = spark.conf.get("spike.source_table", "bosch_poc.spike.change_feed_real")
TARGET = "entities_real"

# Kept on the row but excluded from version comparison: these differ on every delivery, so
# tracking them would make every resend look like a change. `_row_hash` is deliberately NOT
# here -- it is what reproduces V1's rule that a change to ANY source column mints a version,
# including columns that were never promoted out of `payload`.
UNTRACKED = [
    "_ingest_seq", "_ingest_ts", "_batch_id", "_source_file", "_load_mode", "_corrupt_record",
]

# COMMAND ----------


@dp.view(name="change_feed_real")
def change_feed_real():
    return spark.readStream.table(SOURCE_TABLE)


# COMMAND ----------

dp.create_streaming_table(TARGET)

# Keys match V1's window partition. entity_key already hashes project_id in, but keeping both
# makes the grain explicit and costs nothing.
dp.create_auto_cdc_flow(
    target=TARGET,
    source="change_feed_real",
    keys=["project_id", "entity_key"],
    sequence_by=col("_ingest_seq"),
    apply_as_deletes=expr("op = 'DELETE'"),
    except_column_list=["op"],
    track_history_except_column_list=UNTRACKED,
    stored_as_scd_type=2,
)
