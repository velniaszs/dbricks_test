# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Build silver (SCD2)
# MAGIC
# MAGIC One physical SCD2 table: typed generic columns + `payload` for the project-specific tail.
# MAGIC The build is a **pure function of bronze plus a pinned mapping release** -- it never reads
# MAGIC the existing silver table, which is what lets a full rebuild and an incremental run agree.

# COMMAND ----------

import os
import sys

_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path[:0] = [_here, os.getcwd()]
import poc_config as cfg
import silver_builder as sb

dbutils.widgets.text("mapping_ver", "2", "Mapping release")
MAPPING_VER = int(dbutils.widgets.get("mapping_ver"))
print(f"building with mapping release {MAPPING_VER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the catalog generates

# COMMAND ----------

rows = sb.load_release(spark, MAPPING_VER)
exprs, generics = sb.build_select(rows)

for e in exprs:
    print(e, "\n")

# COMMAND ----------

# Snapshot the watermark *before* reading bronze, and materialise it -- a lazy view would
# re-evaluate at MERGE time and pick up files that landed after the build started. Understating
# a cursor only causes harmless reprocessing; overstating it silently skips rows forever.
watermark = spark.sql(f"""
    SELECT project_id, max(_ingest_seq) AS built_seq
    FROM {cfg.BRONZE_TABLE} GROUP BY project_id
""").collect()

spark.createDataFrame(
    [(r.project_id, r.built_seq) for r in watermark],
    "project_id string, built_seq bigint",
).createOrReplaceTempView("_build_watermark")

silver = sb.build_silver(spark, mapping_ver=MAPPING_VER, target=cfg.SILVER_TABLE)

print(f"rows: {silver.count()}")
display(silver.orderBy("project_id", "entity_key", "version_no").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD2 sanity checks
# MAGIC
# MAGIC Unchanged rows repeat in every full load but must not create versions, and the delta load
# MAGIC in the middle must not be mistaken for a full one -- if it were, every entity it omits
# MAGIC would be wrongly tombstoned.

# COMMAND ----------

display(spark.sql(f"""
    SELECT project_id,
           count(*)                        AS row_versions,
           count(DISTINCT entity_key)      AS entities,
           count_if(is_current)            AS current_rows,
           count_if(_change_reason = 'deleted') AS tombstones,
           max(version_no)                 AS max_version
    FROM {cfg.SILVER_TABLE}
    GROUP BY ALL
    ORDER BY project_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deletes
# MAGIC
# MAGIC The last delivery was a full load with objects removed. Each one must have exactly one
# MAGIC tombstone version, and that version must be its latest -- so the entity has **no**
# MAGIC `is_current` row at all, which is what keeps it out of every current view.

# COMMAND ----------

expected = cfg.DELETED_OBJECTS * len(cfg.PROJECTS)
actual = spark.sql(f"""
    SELECT count(*) AS c FROM {cfg.SILVER_TABLE}
    WHERE _change_reason = 'deleted' AND valid_to = TIMESTAMP'{cfg.INFINITY}'
""").collect()[0]["c"]

print(f"expected tombstones: {expected}   actual: {actual}")
assert actual == expected, "delete detection is wrong -- check _load_mode in bronze"

display(spark.sql(f"""
    SELECT project_id, object_id, version_no, valid_from, valid_to, is_current, _change_reason
    FROM {cfg.SILVER_TABLE}
    WHERE _change_reason = 'deleted'
    ORDER BY project_id, object_id
"""))

# COMMAND ----------

# Must return zero rows. Dropping is_deleted weakens "exactly one current row per entity" to
# "at most one", so the open-interval count carries the strict half of the invariant instead.
display(spark.sql(f"""
    WITH per_entity AS (
      SELECT entity_key,
             count_if(is_current)                                AS currents,
             count_if(valid_to = TIMESTAMP'{cfg.INFINITY}')      AS open_intervals,
             count(*)                                            AS versions,
             max(version_no)                                     AS max_ver
      FROM {cfg.SILVER_TABLE}
      GROUP BY entity_key
    )
    SELECT * FROM per_entity
    WHERE currents > 1 OR open_intervals <> 1 OR versions <> max_ver
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resent rows collapse
# MAGIC
# MAGIC The deltas deliberately contain unchanged rows -- AAS Doors does not promise minimal
# MAGIC deltas. Each one must land in bronze and then vanish, because its `_row_hash` matches the
# MAGIC version already open. If any entity has two consecutive versions with the same hash, the
# MAGIC collapse is broken and every resend is inflating history.

# COMMAND ----------

delivered = spark.sql(f"""
    SELECT count(*) AS c FROM {cfg.BRONZE_TABLE} WHERE _load_mode = 'delta'
""").collect()[0]["c"]

repeats = spark.sql(f"""
    WITH chain AS (
      SELECT entity_key, version_no, _row_hash,
             lag(_row_hash) OVER (PARTITION BY entity_key ORDER BY version_no) AS prev_hash
      FROM {cfg.SILVER_TABLE}
    )
    SELECT count(*) AS c FROM chain WHERE prev_hash = _row_hash
""").collect()[0]["c"]

versions_from_delta = spark.sql(f"""
    SELECT count(*) AS c FROM {cfg.SILVER_TABLE} WHERE _load_mode = 'delta'
""").collect()[0]["c"]

print(f"delta rows delivered: {delivered}   versions minted: {versions_from_delta}   "
      f"collapsed as no-ops: {delivered - versions_from_delta}")
assert repeats == 0, "consecutive identical hashes -- lag() collapse is not working"
assert versions_from_delta < delivered, "no delta row collapsed -- resends are re-versioning"

# COMMAND ----------

# An entity that actually changed, showing its full timeline.
display(spark.sql(f"""
    WITH multi AS (
      SELECT entity_key FROM {cfg.SILVER_TABLE}
      GROUP BY entity_key HAVING count(*) > 1 LIMIT 1
    )
    SELECT s.project_id, s.object_id, s.status, s.level, s.version_no,
           s.valid_from, s.valid_to, s.is_current, s._change_reason
    FROM {cfg.SILVER_TABLE} s JOIN multi USING (entity_key)
    ORDER BY s.version_no
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advance the build cursors
# MAGIC
# MAGIC The cursor records the highest **bronze** `_ingest_seq` this build consumed -- not the
# MAGIC highest that reached silver, which is lower because unchanged rows collapse away.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {cfg.STATE_TABLE} t
    USING _build_watermark s ON t.project_id = s.project_id
    WHEN MATCHED THEN UPDATE SET t.last_built_seq = s.built_seq
""")

display(spark.table(cfg.STATE_TABLE).orderBy("project_id"))
