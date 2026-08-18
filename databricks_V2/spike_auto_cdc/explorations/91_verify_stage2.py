# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2 verification: AUTO CDC output vs V1 silver
# MAGIC
# MAGIC Run after `04_build_silver.py` (V1) and the stage 2 pipeline have both produced output from
# MAGIC the **same** bronze contents.
# MAGIC
# MAGIC One structural difference is expected and is not a failure: V1 writes a tombstone **row**
# MAGIC for a deleted entity (`_change_reason = 'deleted'`, `is_current = false`), whereas `AUTO CDC`
# MAGIC closes the previous version and writes nothing. So AUTO CDC should have exactly one fewer
# MAGIC row per deletion. The assertions below encode that relationship rather than raw equality.
# MAGIC
# MAGIC Values are compared without the `_dq_status` filter, because the AUTO CDC path has no DQ
# MAGIC step yet.

# COMMAND ----------

import os
import sys

_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
V1_PATH = os.path.normpath(os.path.join(_here, "..", "..", "..", "databricks_v1"))
sys.path[:0] = [V1_PATH]

import poc_config as cfg
import silver_builder as sb

CDC = "bosch_poc.spike.entities_real"

mapping_ver = spark.sql(
    f"SELECT mapping_ver FROM {cfg.RELEASE_TABLE} WHERE is_current"
).collect()[0]["mapping_ver"]
_, promoted = sb.build_select(sb.load_release(spark, mapping_ver))

v1 = spark.table(cfg.SILVER_TABLE)
cdc = spark.table(CDC)

GRAIN = ["project_id", "entity_key"]
VALUES = GRAIN + promoted

# COMMAND ----------

# MAGIC %md
# MAGIC ## R1 -- same entities

# COMMAND ----------

v1_entities = v1.select(*GRAIN).distinct()
cdc_entities = cdc.select(*GRAIN).distinct()

only_v1 = v1_entities.exceptAll(cdc_entities).count()
only_cdc = cdc_entities.exceptAll(v1_entities).count()

assert only_v1 == 0 and only_cdc == 0, \
    f"R1 FAILED: {only_v1} entities only in V1, {only_cdc} only in AUTO CDC"
print(f"R1 ok -- {v1_entities.count()} entities in both")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R2 -- row counts differ by exactly the tombstone count

# COMMAND ----------

v1_rows = v1.count()
v1_tombstones = v1.where("_change_reason = 'deleted'").count()
cdc_rows = cdc.count()

print(f"V1: {v1_rows} rows ({v1_tombstones} tombstones)   AUTO CDC: {cdc_rows} rows")
assert cdc_rows == v1_rows - v1_tombstones, \
    f"R2 FAILED: expected {v1_rows - v1_tombstones}, got {cdc_rows}"
print("R2 ok -- version counts reconcile")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R3 -- per-entity version chains match
# MAGIC
# MAGIC The no-op collapse lives here. If AUTO CDC mints versions for unchanged resends, or fails
# MAGIC to mint one for a real change, this is where it shows up.

# COMMAND ----------

v1_counts = (v1.where("_change_reason <> 'deleted'")
               .groupBy(*GRAIN).count().withColumnRenamed("count", "v1_versions"))
cdc_counts = cdc.groupBy(*GRAIN).count().withColumnRenamed("count", "cdc_versions")

mismatched = (v1_counts.join(cdc_counts, on=GRAIN, how="full_outer")
                       .where("coalesce(v1_versions, -1) <> coalesce(cdc_versions, -1)"))

n_mismatched = mismatched.count()
if n_mismatched:
    display(mismatched.limit(30))
assert n_mismatched == 0, f"R3 FAILED: {n_mismatched} entities have different version counts"
print("R3 ok -- every entity has the same number of versions in both")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R4 -- current state is identical
# MAGIC
# MAGIC The single most important check: what a consumer would actually read.

# COMMAND ----------

v1_current = v1.where("is_current").select(*VALUES)
cdc_current = cdc.where("__END_AT IS NULL").select(*VALUES)

drift_v1 = v1_current.exceptAll(cdc_current)
drift_cdc = cdc_current.exceptAll(v1_current)
n_drift = drift_v1.count() + drift_cdc.count()

if n_drift:
    print("in V1 but not AUTO CDC:")
    display(drift_v1.limit(20))
    print("in AUTO CDC but not V1:")
    display(drift_cdc.limit(20))

assert n_drift == 0, f"R4 FAILED: {n_drift} current rows differ"
print(f"R4 ok -- {v1_current.count()} current rows identical")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R5 -- deleted entities have no current row in either

# COMMAND ----------

deleted_keys = v1.where("_change_reason = 'deleted'").select(*GRAIN).distinct()

still_current = deleted_keys.join(
    cdc.where("__END_AT IS NULL").select(*GRAIN), on=GRAIN, how="inner"
).count()

assert still_current == 0, \
    f"R5 FAILED: {still_current} deleted entities still current in AUTO CDC"
print(f"R5 ok -- all {deleted_keys.count()} deleted entities closed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R6 -- version ordering agrees
# MAGIC
# MAGIC `__START_AT` carries the sequencing value, so it should reproduce V1's `_ingest_seq`
# MAGIC ordering exactly for the rows that survived the collapse.

# COMMAND ----------

v1_seqs = (v1.where("_change_reason <> 'deleted'")
             .selectExpr(*GRAIN, "_ingest_seq AS seq"))
cdc_seqs = cdc.selectExpr(*GRAIN, "__START_AT AS seq")

order_drift = v1_seqs.exceptAll(cdc_seqs).count() + cdc_seqs.exceptAll(v1_seqs).count()

if order_drift:
    display(v1_seqs.exceptAll(cdc_seqs).limit(20))
assert order_drift == 0, f"R6 FAILED: {order_drift} version boundaries differ"
print("R6 ok -- version boundaries land on the same ingest sequence numbers")

# COMMAND ----------

print("Stage 2 passed. AUTO CDC reproduces V1 silver from the same bronze.")
