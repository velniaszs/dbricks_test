# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Rebuild silver from bronze
# MAGIC
# MAGIC The requirement: **silver must be recreatable from bronze using the mappings in force at
# MAGIC that time.** This notebook proves it twice.
# MAGIC
# MAGIC 1. Rebuild at the *current* release into a side table and diff against live silver.
# MAGIC    Zero rows both ways = the incremental table has no drift.
# MAGIC 2. Rebuild at release **1** and show the pre-`title` shape comes back.

# COMMAND ----------

import os
import sys

_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path[:0] = [_here, os.getcwd()]
import poc_config as cfg
import silver_builder as sb

REBUILD_V2 = cfg.full_name(cfg.SILVER, "entities__rebuild_v2")
REBUILD_V1 = cfg.full_name(cfg.SILVER, "entities__rebuild_v1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Rebuild at the current release -- must be identical

# COMMAND ----------

sb.build_silver(spark, mapping_ver=2, target=REBUILD_V2, build_id="rebuild-v2")

# Non-deterministic-by-design columns are excluded: they record *when the build ran*,
# not what the data is.
VOLATILE = "_build_id, _committed_at"

missing = spark.sql(f"SELECT * EXCEPT ({VOLATILE}) FROM {cfg.SILVER_TABLE} EXCEPT SELECT * EXCEPT ({VOLATILE}) FROM {REBUILD_V2}")
extra = spark.sql(f"SELECT * EXCEPT ({VOLATILE}) FROM {REBUILD_V2} EXCEPT SELECT * EXCEPT ({VOLATILE}) FROM {cfg.SILVER_TABLE}")

print(f"in live but not in rebuild : {missing.count()}")
print(f"in rebuild but not in live : {extra.count()}")
assert missing.count() == 0 and extra.count() == 0, "rebuild diverged -- investigate before trusting silver"
print("PASS - silver is reproducible from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rebuild at release 1 -- the older mapping comes back
# MAGIC
# MAGIC `title` was promoted in release 2, so a release-1 rebuild must not have the column.
# MAGIC The value is still there in `payload`; only the *projection* changed.

# COMMAND ----------

sb.build_silver(spark, mapping_ver=1, target=REBUILD_V1, build_id="rebuild-v1")

v1_cols = set(spark.table(REBUILD_V1).columns)
v2_cols = set(spark.table(REBUILD_V2).columns)

print(f"release 1 columns : {sorted(v1_cols)}")
print(f"only in release 2 : {sorted(v2_cols - v1_cols)}")
assert "title" not in v1_cols and "title" in v2_cols
print("PASS - the mapping in force at the time is what was replayed")

# COMMAND ----------

# The SCD2 timeline itself must be unaffected by the mapping release: version boundaries
# come from _row_hash over the raw source, not from which columns we chose to promote.
display(spark.sql(f"""
    SELECT 'release 1' AS build, count(*) AS versions, count(DISTINCT entity_key) AS entities FROM {REBUILD_V1}
    UNION ALL
    SELECT 'release 2',          count(*),             count(DISTINCT entity_key)            FROM {REBUILD_V2}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Blue/green cutover -- design, not implemented
# MAGIC
# MAGIC Normal operation writes **in place**: one job, one writer, no contention, so there is no
# MAGIC swap and no view indirection. The swap exists only for a full rebuild, which lands in a
# MAGIC side table, is validated as above, and only then takes over.
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE silver.entities         RENAME TO silver.entities__old;
# MAGIC ALTER TABLE silver.entities__rebuild RENAME TO silver.entities;
# MAGIC ```
# MAGIC
# MAGIC Three rules that are easy to get wrong:
# MAGIC
# MAGIC 1. **Pause the incremental silver build for the duration.** Ingest can keep running --
# MAGIC    it is append-only and the cursors do not move -- but any increment merged into the
# MAGIC    live table while the rebuild is in flight is thrown away by the rename.
# MAGIC 2. **Swap `meta.project_state.last_built_seq` in the same step**, recomputed from the
# MAGIC    rebuilt table. Otherwise the next incremental run resumes from a cursor that does not
# MAGIC    describe the data underneath it.
# MAGIC 3. The two renames are **not atomic** -- there is a short window with no
# MAGIC    `silver.entities`. Acceptable for a planned, rare operation. If it ever isn't, point
# MAGIC    gold at a view over a physical `entities_a` / `entities_b` pair and make the cutover a
# MAGIC    `CREATE OR REPLACE VIEW`.
# MAGIC
# MAGIC Prefer rebuilding **one project at a time** (`WHERE project_id = ...`): projects share no
# MAGIC state in `assign_versions`, so a single project can be repaired without a global outage.
# MAGIC
# MAGIC Views over `silver.entities` resolve by name, so gold follows automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cleanup

# COMMAND ----------

for table in (REBUILD_V1, REBUILD_V2):
    spark.sql(f"DROP TABLE IF EXISTS {table}")
print("dropped rebuild side tables")
