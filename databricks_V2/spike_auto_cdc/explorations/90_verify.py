# Databricks notebook source
# MAGIC %md
# MAGIC # Spike verification
# MAGIC
# MAGIC Run after the pipeline. Every assertion must pass for the spike to succeed.
# MAGIC For Q5, run **Full refresh all** on the pipeline and execute this notebook again --
# MAGIC the fingerprint printed at the end must be unchanged.

# COMMAND ----------

TARGET = "bosch_poc.spike.entity_history"

hist = spark.table(TARGET)
display(hist.orderBy("entity_id", "__START_AT"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 -- no-op resends collapse
# MAGIC
# MAGIC C was resent byte-identical in the seq 2 delta. This is the bug found on 2026-08-14,
# MAGIC where 86 delta rows minted 86 versions instead of ~36.

# COMMAND ----------

c_versions = hist.where("entity_id = 'C'").count()
assert c_versions == 1, f"Q1 FAILED: C has {c_versions} versions, expected 1"
print("Q1 ok -- unchanged resend did not mint a version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 -- deletion by absence closes the chain
# MAGIC
# MAGIC D was in the seq 1 full load and absent from the seq 3 full load.

# COMMAND ----------

d = hist.where("entity_id = 'D'").collect()
assert len(d) == 1, f"Q2 FAILED: D has {len(d)} versions, expected 1"
assert d[0]["__END_AT"] is not None, "Q2 FAILED: D's version was never closed"
assert str(d[0]["__END_AT"]) == "3", f"Q2 FAILED: D closed at {d[0]['__END_AT']}, expected 3"
print("Q2 ok -- absent entity tombstoned, zero current rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 -- full and delta interleave in one chain
# MAGIC
# MAGIC B changed in the seq 2 delta and again in the seq 3 full load.

# COMMAND ----------

b = hist.where("entity_id = 'B'").orderBy("__START_AT").collect()
chain = [r["attr_a"] for r in b]
assert len(b) == 3, f"Q3 FAILED: B has {len(b)} versions, expected 3"
assert chain == ["a1", "a2", "a3"], f"Q3 FAILED: B value chain is {chain}"
assert b[-1]["__END_AT"] is None, "Q3 FAILED: B's latest version is not open"
print("Q3 ok -- delta and full events ordered into one version chain")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4 -- whole-table shape

# COMMAND ----------

total = hist.count()
current = hist.where("__END_AT IS NULL").count()
current_ids = sorted(r.entity_id for r in hist.where("__END_AT IS NULL").collect())

assert total == 7, f"Q4 FAILED: {total} rows, expected 7"
assert current == 4, f"Q4 FAILED: {current} current rows, expected 4"
assert current_ids == ["A", "B", "C", "E"], f"Q4 FAILED: current entities {current_ids}"
print("Q4 ok -- 7 versions, 4 current")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5 -- deterministic rebuild
# MAGIC
# MAGIC Print the fingerprint, run **Full refresh all** on the pipeline, run this cell again.
# MAGIC The two values must match, otherwise rebuild-from-bronze is not reproducible and the
# MAGIC whole reason bronze exists is undermined.

# COMMAND ----------

fingerprint = spark.sql(f"""
SELECT md5(concat_ws('|', collect_list(row_repr))) AS fingerprint
FROM (
  SELECT concat_ws(',', entity_id, attr_a, attr_b,
                   coalesce(cast(__START_AT AS string), 'null'),
                   coalesce(cast(__END_AT   AS string), 'null')) AS row_repr
  FROM {TARGET}
  ORDER BY entity_id, __START_AT
)
""").collect()[0]["fingerprint"]

print(f"Q5 fingerprint: {fingerprint}")

# COMMAND ----------

print("Q1-Q4 passed. Compare the Q5 fingerprint across a full refresh before deciding.")
