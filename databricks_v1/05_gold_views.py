# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold views
# MAGIC
# MAGIC Gold is logical. Databricks views are inlined into the query plan, so filtering through a
# MAGIC view produces the same physical plan as writing the `VARIANT` extraction by hand -- there
# MAGIC is no reason to materialise a second copy of the data.
# MAGIC
# MAGIC The per-project wide views are **generated** from `meta.column_catalog`.

# COMMAND ----------

import os
import sys

_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path[:0] = [_here, os.getcwd()]
import poc_config as cfg

GOLD = f"{cfg.CATALOG}.{cfg.GOLD}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cross-project current state

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.entities_current AS
SELECT entity_key, project_id, object_id, level, status, owner, modified_ts,
       valid_from, version_no
FROM {cfg.SILVER_TABLE}
WHERE valid_to = TIMESTAMP'{cfg.INFINITY}' AND is_current AND _dq_status <> 'quarantine'
""")

display(spark.sql(f"SELECT * FROM {GOLD}.entities_current ORDER BY project_id, object_id LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Per-project wide views, generated from the catalog

# COMMAND ----------

catalog_rows = spark.sql(f"""
    SELECT project_id, source_column
    FROM {cfg.CATALOG_TABLE}
    WHERE NOT is_promoted AND superseded_at > current_timestamp()
    ORDER BY project_id, source_column
""").collect()

unique_by_project = {}
for r in catalog_rows:
    unique_by_project.setdefault(r.project_id, []).append(r.source_column)

for project_id, source_columns in unique_by_project.items():
    extractions = ",\n       ".join(
        f"try_variant_get(payload, '$.{c}', 'STRING') AS {c.lower()}" for c in source_columns
    )
    view = f"{GOLD}.proj_{project_id.lower()}"
    spark.sql(f"""
CREATE OR REPLACE VIEW {view} AS
SELECT entity_key, object_id, level, status, owner, modified_ts,
       {extractions},
       valid_from, valid_to, version_no
FROM {cfg.SILVER_TABLE}
WHERE project_id = '{project_id}'
  AND valid_to = TIMESTAMP'{cfg.INFINITY}' AND is_current
  AND _dq_status <> 'quarantine'
""")
    print(f"created {view}  (+{len(source_columns)} columns from payload)")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {GOLD}.proj_ferrari LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. History and point-in-time

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.entities_history AS
SELECT * EXCEPT (_row_hash, _ingest_seq, _build_id)
FROM {cfg.SILVER_TABLE}
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {GOLD}.entities_asof(as_of TIMESTAMP)
RETURNS TABLE
RETURN
  SELECT entity_key, project_id, object_id, level, status, owner, version_no,
         valid_from, valid_to
  FROM {cfg.SILVER_TABLE}
  WHERE as_of >= valid_from AND as_of < valid_to
    AND _change_reason <> 'deleted'
""")

display(spark.sql(f"""
    SELECT project_id, count(*) AS entities_as_of_feb
    FROM {GOLD}.entities_asof(TIMESTAMP'2026-02-10 00:00:00')
    GROUP BY ALL ORDER BY project_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC A deleted entity has **no** `is_current` row, which is what removes it from every current
# MAGIC view without a second flag. Its history is untouched, and the as-of function still returns
# MAGIC it for any point in time before the deletion -- hence the explicit `_change_reason` filter
# MAGIC there, since the tombstone's interval runs to infinity like any other open version.

# COMMAND ----------

display(spark.sql(f"""
    SELECT project_id, object_id, version_no, valid_from, valid_to, is_current, _change_reason
    FROM {cfg.SILVER_TABLE}
    WHERE entity_key IN (
      SELECT entity_key FROM {cfg.SILVER_TABLE} WHERE _change_reason = 'deleted'
    )
    ORDER BY project_id, object_id, version_no
    LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The cost of skipping value harmonisation
# MAGIC
# MAGIC The `level` column is shared, the values are not. A cross-project `GROUP BY level`
# MAGIC groups by **encoding, not meaning** -- and a dashboard filtering `level = 'System'`
# MAGIC silently returns nothing for two of the three projects.
# MAGIC
# MAGIC This is the accepted trade-off of the names-only mapping. Run this query per release: the
# MAGIC moment consumers start hard-coding `IN` lists, it is time to add a value-map table.

# COMMAND ----------

display(spark.sql(f"""
    SELECT level, count(*) AS rows, collect_set(project_id) AS projects
    FROM {GOLD}.entities_current
    GROUP BY level
    ORDER BY level
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'level = System only'          AS query, count(*) AS rows FROM {GOLD}.entities_current WHERE level = 'System'
    UNION ALL
    SELECT 'level IN (System, SYS, L1)',  count(*)          FROM {GOLD}.entities_current WHERE level IN ('System','SYS','L1')
"""))
