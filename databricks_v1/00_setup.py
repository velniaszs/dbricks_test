# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC
# MAGIC Creates the catalog, schemas and landing volume. Run once, on **serverless** compute.
# MAGIC
# MAGIC If `CREATE CATALOG` fails with a permissions error, set `poc_config.CATALOG` to an
# MAGIC existing catalog you can write to (often `workspace` or `main`) and re-run.

# COMMAND ----------

import os
import sys

# A notebook's cwd is not reliably its own folder, so resolve it from the notebook context.
_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path[:0] = [_here, os.getcwd()]

print(f"notebook folder : {_here}")
print(f"contents        : {sorted(os.listdir(_here))}")

# COMMAND ----------

import poc_config as cfg

print(f"catalog: {cfg.CATALOG}")
print(f"landing: {cfg.LANDING_ROOT}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.CATALOG}")

for schema in (cfg.BRONZE, cfg.SILVER, cfg.GOLD, cfg.META):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.CATALOG}.{cfg.BRONZE}.{cfg.VOLUME}")

display(spark.sql(f"SHOW SCHEMAS IN {cfg.CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze table
# MAGIC
# MAGIC One table for every project. Only routing / ordering / hash columns are real; all data
# MAGIC lands in `payload` as a `VARIANT` of raw strings, so projects with completely different
# MAGIC column sets coexist with no NULL padding and no shared schema to evolve.
# MAGIC
# MAGIC `_load_mode` records whether a file was a **full** load or a **delta**. Silver needs it to
# MAGIC interpret absence: an entity missing from a full load has been deleted, whereas one missing
# MAGIC from a delta load is simply unchanged.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.BRONZE_TABLE} (
  project_id     STRING    NOT NULL,
  source_system  STRING    NOT NULL,

  _ingest_ts     TIMESTAMP NOT NULL,
  _ingest_seq    BIGINT    NOT NULL,
  _batch_id      STRING    NOT NULL,
  _source_file   STRING    NOT NULL,
  _file_modified TIMESTAMP,
  _file_row_num  BIGINT,
  _load_mode     STRING    NOT NULL,

  _row_hash      STRING    NOT NULL,

  payload        VARIANT   NOT NULL,
  _schema_ver    STRING,
  _corrupt_record STRING
)
USING DELTA
CLUSTER BY (project_id, _ingest_ts)
TBLPROPERTIES (
  delta.dataSkippingNumIndexedCols = 0,
  delta.dataSkippingStatsColumns   = 'project_id,_ingest_ts,_ingest_seq,_batch_id,_source_file,_load_mode',
  delta.enableDeletionVectors      = true,
  delta.enableChangeDataFeed       = true,
  delta.columnMapping.mode         = 'name'
)
""")

display(spark.sql(f"DESCRIBE TABLE {cfg.BRONZE_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata tables
# MAGIC
# MAGIC `column_catalog` maps source column -> generic column per project and carries the parse
# MAGIC rules; `mapping_release` versions it so any past state can be replayed. There is
# MAGIC deliberately no vocabulary table and no value-mapping table -- a generic column is a
# MAGIC shared *name*, values are stored exactly as the source sends them.
# MAGIC
# MAGIC `schema_registry` records the actual column list behind every `_schema_ver` hash, so a
# MAGIC project silently adding or dropping a column is a diff between two rows rather than a
# MAGIC discovery six weeks later. `project_state` holds the per-project cursors: ingest advances
# MAGIC `last_ingest_seq`, the silver build advances `last_built_seq`, and neither blocks the other.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.CATALOG_TABLE} (
  project_id      STRING  NOT NULL,
  source_column   STRING  NOT NULL,
  generic_column  STRING,
  is_promoted     BOOLEAN NOT NULL,
  is_business_key BOOLEAN NOT NULL,
  precedence      INT     NOT NULL,
  target_type     STRING,
  parse_format    STRING,
  description     STRING,
  recorded_at     TIMESTAMP NOT NULL,
  superseded_at   TIMESTAMP NOT NULL
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.RELEASE_TABLE} (
  mapping_ver INT       NOT NULL,
  released_at TIMESTAMP NOT NULL,
  description STRING,
  is_current  BOOLEAN   NOT NULL
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.REGISTRY_TABLE} (
  project_id   STRING NOT NULL,
  schema_ver   STRING NOT NULL,
  columns      ARRAY<STRING>,
  column_count INT,
  first_seen   TIMESTAMP,
  last_seen    TIMESTAMP,
  first_file   STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.STATE_TABLE} (
  project_id      STRING NOT NULL,
  last_ingest_seq BIGINT NOT NULL,
  last_built_seq  BIGINT NOT NULL,
  last_ingest_ts  TIMESTAMP,
  last_build_id   STRING
) USING DELTA
""")

print("setup complete")
