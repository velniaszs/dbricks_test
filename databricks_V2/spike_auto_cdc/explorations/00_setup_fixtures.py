# Databricks notebook source
# MAGIC %md
# MAGIC # Spike fixtures
# MAGIC
# MAGIC Builds `bosch_poc.spike.bronze_events` (stands in for the real bronze table) and
# MAGIC `bosch_poc.spike.bronze_change_feed` (the new normalisation step under test).
# MAGIC
# MAGIC Deliberately tiny. Five entities, three extracts. See README for the fixture table.

# COMMAND ----------

CATALOG = "bosch_poc"
SCHEMA = "spike"
EVENTS = f"{CATALOG}.{SCHEMA}.bronze_events"
FEED = f"{CATALOG}.{SCHEMA}.bronze_change_feed"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze fixture
# MAGIC
# MAGIC `seq 1` full, `seq 2` delta, `seq 3` full. C is resent unchanged in the delta; D is
# MAGIC absent from the second full load and must end up tombstoned.

# COMMAND ----------

rows = [
    # seq 1 -- full: initial population
    ("A", "a1", "b1", 1, "full"),
    ("B", "a1", "b1", 1, "full"),
    ("C", "a1", "b1", 1, "full"),
    ("D", "a1", "b1", 1, "full"),
    # seq 2 -- delta: B changed, C resent identical, E new
    ("B", "a2", "b1", 2, "delta"),
    ("C", "a1", "b1", 2, "delta"),
    ("E", "a1", "b1", 2, "delta"),
    # seq 3 -- full: B changed again, D gone
    ("A", "a1", "b1", 3, "full"),
    ("B", "a3", "b1", 3, "full"),
    ("C", "a1", "b1", 3, "full"),
    ("E", "a1", "b1", 3, "full"),
]

(spark.createDataFrame(rows, "entity_id string, attr_a string, attr_b string, extract_seq int, load_mode string")
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(EVENTS))

display(spark.table(EVENTS).orderBy("extract_seq", "entity_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change feed
# MAGIC
# MAGIC Every bronze row becomes an `UPSERT`. A `DELETE` is derived for each key that was known
# MAGIC before a full load but is absent from it -- this is `deletion_events()` reduced to a set
# MAGIC difference, which is the part of the current implementation that was never the problem.
# MAGIC
# MAGIC The feed is append-only so the pipeline can stream from it.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FEED} AS
WITH upserts AS (
  SELECT entity_id, attr_a, attr_b, extract_seq AS seq, 'UPSERT' AS op
  FROM {EVENTS}
),
full_extracts AS (
  SELECT DISTINCT extract_seq FROM {EVENTS} WHERE load_mode = 'full'
),
deletes AS (
  SELECT DISTINCT
         known.entity_id,
         CAST(NULL AS STRING) AS attr_a,
         CAST(NULL AS STRING) AS attr_b,
         f.extract_seq       AS seq,
         'DELETE'            AS op
  FROM full_extracts f
  JOIN {EVENTS} known
    ON known.extract_seq < f.extract_seq
  WHERE NOT EXISTS (
    SELECT 1 FROM {EVENTS} present
    WHERE present.load_mode  = 'full'
      AND present.extract_seq = f.extract_seq
      AND present.entity_id   = known.entity_id
  )
)
SELECT * FROM upserts
UNION ALL
SELECT * FROM deletes
""")

display(spark.table(FEED).orderBy("seq", "entity_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expect 12 rows: 11 upserts and exactly one delete (D at seq 3).

# COMMAND ----------

assert spark.table(FEED).count() == 12, "unexpected change feed size"

deletes = [r.entity_id for r in spark.table(FEED).where("op = 'DELETE'").collect()]
assert deletes == ["D"], f"expected only D to be deleted, got {deletes}"

print("fixtures ready")
