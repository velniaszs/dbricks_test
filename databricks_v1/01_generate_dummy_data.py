# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate dummy AAS Doors extracts
# MAGIC
# MAGIC One file per project per delivery, alternating **full** and **delta** loads. A full load
# MAGIC lists every live object; a delta load lists only what changed since the last delivery.
# MAGIC Files land under `<project>/<full|delta>/` -- the folder is the load-mode contract, and
# MAGIC the file name is treated as opaque.
# MAGIC
# MAGIC What the data deliberately contains:
# MAGIC
# MAGIC - the same concepts under different column names per project (`Status_Ferrari` /
# MAGIC   `Status_Mclaren` / `State_Alp`),
# MAGIC - the same concepts under **different value encodings** (`System` / `SYS` / `L1`),
# MAGIC - different timestamp formats per project,
# MAGIC - unchanged rows repeated in every full load (must NOT create SCD2 versions),
# MAGIC - changed rows delivered both ways (must create versions),
# MAGIC - objects **missing from the final full load** (must be closed off as deleted),
# MAGIC - a new column appearing mid-stream for one project only.
# MAGIC
# MAGIC Generation is seeded, so re-running produces byte-identical files.

# COMMAND ----------

import csv
import os
import random
import sys
import time
from datetime import datetime, timedelta

_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path[:0] = [_here, os.getcwd()]
import poc_config as cfg

N_OBJECTS = 60
CHANGE_RATE = {0: 0.0, 1: 0.20, 2: 0.10}
SEED = 20260813

# COMMAND ----------


def initial_state(project_id, conf, rng, base_ts):
    rows = []
    for i in range(N_OBJECTS):
        rows.append({
            "object_id": f"{project_id[:3]}-{i + 1:04d}",
            "level": conf["levels"][i % len(conf["levels"])],
            "status": rng.choice(conf["statuses"]),
            "title": f"Requirement {i + 1} for {project_id.title()}",
            "owner": rng.choice(conf["owners"]),
            "modified": base_ts + timedelta(hours=i),
            "unique": {c: f"{rng.uniform(10, 200):.1f}" for c in conf["unique"]},
        })
    return rows


def mutate(rows, conf, rng, rate, ts):
    """Returns the objects that actually changed."""
    touched = []
    for row in rows:
        if rng.random() >= rate:
            continue
        row["status"] = rng.choice(conf["statuses"])
        first_unique = conf["unique"][0]
        row["unique"][first_unique] = f"{rng.uniform(10, 200):.1f}"
        row["modified"] = ts
        touched.append(row)
    return touched


def delta_payload(state, touched, rng):
    """A delta carries the changed rows plus a helping of unchanged ones -- the source does not
    promise minimality, so the PoC must prove those resends collapse instead of re-versioning."""
    changed = {id(r) for r in touched}
    noops = [r for r in state if id(r) not in changed and rng.random() < cfg.DELTA_NOOP_RATE]
    payload = touched + noops
    rng.shuffle(payload)          # interleaved, so nothing downstream can rely on file order
    return payload, len(noops)


def to_csv_record(row, project_id, conf, extract_idx):
    g = conf["generic"]
    record = {
        g["object_id"]: row["object_id"],
        g["level"]: row["level"],
        g["status"]: row["status"],
        g["title"]: row["title"],
        g["owner"]: row["owner"],
        g["modified_ts"]: row["modified"].strftime(conf["ts_format_py"]),
    }
    record.update(row["unique"])
    # Schema evolution: one project gains a column in the last extract only.
    if project_id == cfg.NEW_COLUMN_PROJECT and extract_idx == len(cfg.EXTRACTS) - 1:
        record[cfg.NEW_COLUMN_NAME] = f"ASIL-{row['object_id'][-1]}"
    return record


# COMMAND ----------

written = []

rngs = {p: random.Random(f"{SEED}-{p}") for p in cfg.PROJECTS}
base_ts = datetime.strptime(cfg.EXTRACTS[0][0], "%Y-%m-%d")
states = {p: initial_state(p, conf, rngs[p], base_ts) for p, conf in cfg.PROJECTS.items()}

# Extract-major, not project-major: every project delivers its 2026-01-05 extract before anyone
# delivers 2026-02-05. Volumes stamp mtime as the wall-clock write time and ignore os.utime, and
# mtime is the only ordering signal ingest has -- so files of one project must land in different
# seconds. Ties between projects are fine, which is why only the outer loop pauses.
for idx, (extract_date, load_mode) in enumerate(cfg.EXTRACTS):
    if idx:
        time.sleep(1.1)

    ts = datetime.strptime(extract_date, "%Y-%m-%d")

    for project_id, conf in cfg.PROJECTS.items():
        rng = rngs[project_id]
        touched = mutate(states[project_id], conf, rng, CHANGE_RATE[idx], ts)

        # Deletions are only observable in a full load, so drop them in the last one.
        if load_mode == "full" and idx == len(cfg.EXTRACTS) - 1:
            states[project_id] = states[project_id][: -cfg.DELETED_OBJECTS]

        state = states[project_id]
        if load_mode == "full":
            emitted, noops = state, 0
        else:
            emitted, noops = delta_payload(state, touched, rng)
        records = [to_csv_record(r, project_id, conf, idx) for r in emitted]
        header = list(records[0].keys())

        # The folder is the load-mode contract; the file name itself carries no meaning.
        directory = cfg.landing_dir(project_id, load_mode)
        os.makedirs(directory, exist_ok=True)
        path = f"{directory}/{project_id.lower()}_{extract_date}.csv"

        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=header)
            writer.writeheader()
            for record in records:
                writer.writerow({k: record.get(k, "") for k in header})

        written.append((project_id, extract_date, load_mode, path, len(records), len(header), noops))

for p, d, m, path, rows, cols, noops in written:
    print(f"{p:8} {d}  {m:5}  rows={rows:4}  cols={cols:2}  resent={noops:4}  {path}")

# COMMAND ----------

first_date, first_mode = cfg.EXTRACTS[0]
print(open(f"{cfg.landing_dir('FERRARI', first_mode)}/ferrari_{first_date}.csv", encoding="utf-8").read()[:600])

# COMMAND ----------

last_date, last_mode = cfg.EXTRACTS[-1]
print(open(f"{cfg.landing_dir('ALPINE', last_mode)}/alpine_{last_date}.csv", encoding="utf-8").read()[:600])
