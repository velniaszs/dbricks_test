# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2: change feed from the real bronze table
# MAGIC
# MAGIC Stage 1 proved the SCD2 semantics on synthetic rows. This one runs the same idea through
# MAGIC the real machinery: `bronze.aas_doors_raw`, the `VARIANT` payload, the bitemporal column
# MAGIC catalog, three projects with different column spellings, and real `entity_key` hashes.
# MAGIC
# MAGIC It **imports V1's `silver_builder` read-only** rather than reimplementing the mapping. That
# MAGIC is deliberate: staging and deletion detection are then provably identical to what V1 does,
# MAGIC so any difference in the result is attributable to `AUTO CDC` alone. Nothing in
# MAGIC `databricks_v1/` is written to.
# MAGIC
# MAGIC **Requires a clean bronze** -- run `00 -> 02` in `databricks_v1/` after the `StructType.add`
# MAGIC fix, otherwise `_row_hash` still carries the bug and no-op collapse cannot be judged.

# COMMAND ----------

import os
import sys

_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
# explorations -> spike_auto_cdc -> databricks_V2 -> repo root
V1_PATH = os.path.normpath(os.path.join(_here, "..", "..", "..", "databricks_v1"))

assert os.path.isdir(V1_PATH), (
    f"V1 modules not found at {V1_PATH}. Set V1_PATH to the folder holding "
    "poc_config.py and silver_builder.py."
)
sys.path[:0] = [V1_PATH]

import poc_config as cfg
import silver_builder as sb

from pyspark.sql import functions as F

# COMMAND ----------

SPIKE_SCHEMA = "bosch_poc.spike"
FEED = f"{SPIKE_SCHEMA}.change_feed_real"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SPIKE_SCHEMA}")

mapping_ver = spark.sql(
    f"SELECT mapping_ver FROM {cfg.RELEASE_TABLE} WHERE is_current"
).collect()[0]["mapping_ver"]

print(f"V1 modules: {V1_PATH}")
print(f"mapping_ver: {mapping_ver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage bronze through the catalog
# MAGIC
# MAGIC Same call `build_silver` makes, so the projected columns and `entity_key` are identical.

# COMMAND ----------

rows = sb.load_release(spark, mapping_ver)
exprs, promoted = sb.build_select(rows)

print(f"promoted columns: {promoted}")

PROVENANCE = [
    "project_id", "_ingest_seq", "_ingest_ts", "_batch_id",
    "_source_file", "_load_mode", "_row_hash", "_corrupt_record",
]

staged = spark.sql(f"""
    SELECT {', '.join(PROVENANCE)}, {', '.join(exprs)}
    FROM {cfg.BRONZE_TABLE}
""")

print(f"staged rows: {staged.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive the change feed
# MAGIC
# MAGIC `deletion_events` is V1's, unchanged -- it is a set difference over bronze and was never
# MAGIC the buggy part. Everything it emits carries `DELETED_HASH`, which is how a tombstone is
# MAGIC told apart from a normal row and turned into a CDC `DELETE`.
# MAGIC
# MAGIC What is *not* here is `assign_versions`. That is the whole point.

# COMMAND ----------

events = staged.unionByName(sb.deletion_events(staged))

feed = events.withColumn(
    "op",
    F.when(F.col("_row_hash") == F.lit(cfg.DELETED_HASH), F.lit("DELETE")).otherwise(F.lit("UPSERT")),
)

(feed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEED))

display(
    spark.table(FEED).groupBy("op", "_load_mode").count().orderBy("op", "_load_mode")
)

# COMMAND ----------

# MAGIC %md
# MAGIC `_ingest_seq` is the sequencing column. `AUTO CDC` requires it to be monotonic with at most
# MAGIC one update per key per value -- check that here rather than discovering it as a silent
# MAGIC mis-ordering later.

# COMMAND ----------

dupes = (
    spark.table(FEED)
    .groupBy("project_id", "entity_key", "_ingest_seq")
    .count()
    .where("count > 1")
)

n_dupes = dupes.count()
if n_dupes:
    display(dupes.limit(20))
raise_msg = (
    f"{n_dupes} (key, seq) collisions -- sequence_by would need struct(_ingest_seq, _row_hash)"
)
assert n_dupes == 0, raise_msg

print(f"feed rows: {spark.table(FEED).count()}")
print("sequencing contract holds")
