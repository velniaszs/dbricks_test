# Databricks Lakehouse Architecture — V2, multi-source framework

**Customer:** Bosch
**Status:** design agreed, implementation starting. Nothing in `databricks_V2/framework/` is built yet.

V1 (`databricks_v1/`) is a working single-entity PoC: one source system, one entity, twenty
tenants. It is described in [databricks_v1/docs/architecture.md](../../databricks_v1/docs/architecture.md)
and stays runnable as the reference implementation and the reconciliation baseline.

V2 generalises it to many source systems and many entities. The analysis behind this document —
what breaks, what holds, and what was verified against Databricks guidance — is
[scaling-to-a-framework.md](scaling-to-a-framework.md). This document does not repeat that
reasoning; it states the resulting design.

> **This is a re-scoping, not a rewrite.** Three things in V1 are scoped to "one entity" and need
> one more level of key. One thing needs to become pluggable. The per-tenant column-renaming
> machinery — the most expensive and least common capability in the codebase — carries over
> unchanged.

---

## What carries over unchanged

These are settled by V1 and are not reopened here. Each is justified in the V1 document.

| Decision | Why it survives generalisation |
|---|---|
| Bronze is append-only, all values `STRING`, data in a `VARIANT` payload | Schema-agnostic ingest is what makes per-stream tables free of per-stream code |
| `_row_hash` is **catalog-free** — one hash over all normalised source values | Version boundaries must be a function of source data alone, never of our modelling |
| Deletes are **tombstone versions**, derived per full-load file by set difference | Physical deletes break rebuildability |
| `is_current` means *latest version AND still present in source* | One predicate for consumers; deleted entities have zero current rows |
| Silver = pure function of (bronze, mapping release) | The rebuild requirement is the reason bronze exists |
| Mapping is **names only**, values passed through as sent | Customer decision, 2026-08-13 |
| Custom historisation (`assign_versions`), not `AUTO CDC` | Decision 2026-08-18 — see below |

### Historisation stays hand-written

`AUTO CDC` was spiked in [spike_auto_cdc](../spike_auto_cdc/README.md) and works. It is not being
adopted. The reason is control rather than capability: version assignment is the most consequential
step in the pipeline, it is where both August 2026 bugs lived, and an explicit merge can be
unit-tested against a fixture and stepped through when a customer disputes a version boundary.

One useful consequence. Because silver is built by notebook tasks rather than pipeline tasks, V2 is
not subject to the documented limit that **a pipeline task inside a `For each` task runs one
iteration at a time regardless of configured concurrency**. Per-stream fan-out can actually run in
parallel.

---

## The three axes

| Axis | Example | V1 | V2 |
|---|---|---|---|
| **Source system** | AAS Doors, SAP, Teamcenter | hardcoded | `source_system` — config |
| **Entity** | requirement, change request, BOM line | hardcoded | `entity` — config |
| **Tenant / variant** | FERRARI, MCLAREN | solved | `tenant_id` — unchanged mechanism |

**A *stream* is `(source_system, entity)`.** It is the unit of ingestion, ordering, parallelism,
retention and failure. Tenants live *inside* a stream as a column, exactly as `project_id` does
today.

That single definition resolves most of V1's scaling problems, because everything V1 scoped
globally is correctly scoped per stream.

---

## Unity Catalog layout

**Catalog per environment**, which is what Databricks recommends (*"catalogs correspond to an
environment scope, team, business unit, or some combination"*).

```
bosch_dev / bosch_test / bosch_prod
├── bronze     aas_doors__requirement, sap__material, …   one table per stream
├── silver     aas_doors__requirement, …                  one table per stream
├── work       intermediate + joined datasets
├── gold       consumer-facing views and materialized views
└── meta       the framework's own state
```

Table naming is `<source_system>__<entity>`, double underscore, resolved from metadata and never
hardcoded. Single underscores are legal inside either part.

**One bronze table per stream**, not one global table. Independent OPTIMIZE / VACUUM / retention,
bounded blast radius, parallel writers. The alternative — one table with an `entity` column —
forces every stream through one writer for sequence assignment.

### Why `work` exists

Databricks' medallion guidance explicitly places joins and enriched datasets in silver, and states
that *"large amounts of historical data are typically accessed in the silver layer and not
materialized in the gold layer"*. An earlier draft of this design required silver to be strictly
single-entity and mechanical; that was stricter than the vendor's own guidance and has been
dropped.

So silver has two zones:

- **`silver`** — one validated, non-aggregated timeline per entity. Never joined, never aggregated.
  This is the layer the rebuild guarantee applies to.
- **`work`** — joins, enrichment, intermediate results. Rebuildable from `silver`, no independent
  history, freely dropped and recomputed.

Gold stays consumer-facing only, and stays mostly logical.

### Landing areas are external volumes

Managed volumes are the default for most data, but landing zones should be **external volumes** on
an external location with **file events** enabled. File events are what make file-arrival triggers
and Auto Loader scale, and they directly address V1's `dbutils.fs.ls` O(n) discovery problem.

---

## Configuration is data, but it lives in git

The trap with "config as data" is untraceable production changes and no dev → prod promotion path.
Delta tables are not git.

The reconciliation: **declarations are files in the repository; a deploy job loads them into `meta`
bitemporally.** Review, history and reproducible rebuilds come from git; runtime lookups and
point-in-time rebuild come from `meta`.

Three layers:

| Layer | Location | Format | Why |
|---|---|---|---|
| **1a — entity definition** | `sources/<source_system>/<entity>.yml` | YAML | Small, structured, human-authored |
| **1b — column mapping** | `mappings/<source_system>/<entity>.csv` | **CSV** | 1300 columns is not hand-editable YAML; it is a table, so it is stored as one |
| **2 — environment** | `environments/<env>.yml` | YAML | Catalog names, volumes, schedules, compute — maps onto bundle `targets` |

Layer 1b being CSV is a deliberate concession to scale: it diffs acceptably in git, opens in Excel
for the analysts who actually know the column meanings, and imports without a parser.

### Entity definition — layer 1a

```yaml
source_system: aas_doors
entity: requirement

landing:
  volume: landing                      # resolved per environment
  path: aas_doors/requirement
  layout: "<tenant>/<load_mode>"       # folder contract, as V1
  format: csv
  options: { header: true }

grain:
  business_key: [object_id]
  tenant_column: project_id

historisation: scd2                    # scd2 | current_only | insert_only | append | snapshot

sequencing:
  source: manifest                     # manifest | file_mtime
  manifest_column: extract_seq

schema_evolution:
  new_column: accept                   # stays in payload, no action
  missing_business_key: fail           # stop the stream loudly

quality:
  required: [object_id, level]
  on_violation: quarantine             # quarantine | drop | fail

retention:
  bronze_days: 3650
```

Everything here is per entity and none of it is code. Onboarding an entity is one YAML file plus
one CSV — no new notebooks, which is the same promise V1 made for adding a project, one level up.

### The `meta` tables

| Table | Holds | V1 equivalent |
|---|---|---|
| `meta.stream` | one row per `(source_system, entity)`: historisation, landing contract, format, policies, retention | `poc_config.py` |
| `meta.entity_column` | target schema per entity | `GENERIC_COLUMNS` in Python |
| `meta.column_catalog` | `(source_system, entity, tenant, source_column) → generic_column`, bitemporal | same, one key level narrower |
| `meta.mapping_release` | releases, `is_current` | unchanged |
| `meta.stream_state` | cursors: `last_ingest_seq`, `last_built_seq` per stream | `meta.project_state` |
| `meta.ingested_file` | path + **content checksum** ledger | `SELECT DISTINCT _source_file` scan |
| `meta.schema_registry` | header behind each `_schema_ver` | unchanged |
| `meta.dq_rule` | declarative rules per entity | hardcoded in `silver_builder` |
| `meta.run_log` | per stream per run: files, rows, versions minted, no-op ratio, cast failures, duration | did not exist |

`meta.ingested_file` fixes a real V1 defect, not just a performance one: the `_source_file` scan
silently mishandles a corrected file redelivered under the same name. Keying on path **and**
checksum makes "same name, new content" a supported case instead of silent data loss.

---

## Ordering: `_ingest_seq` per stream

V1's single-writer constraint traces to one line:

```python
base_seq = spark.sql("SELECT coalesce(max(_ingest_seq), 0) FROM bronze").collect()[0]["s"]
```

A read-modify-write. It is not fundamental. `_ingest_seq`'s actual job is *a total, reproducible
order within one entity's timeline*, and entities never span streams.

**Two changes, in order of preference:**

1. **Derive it.** With a delivery manifest carrying a sequence number:
   `_ingest_seq = delivery_seq * 10^9 + row_num`. No read at all, so no writer serialisation, and
   rebuilds stay stable even when bronze is reprocessed out of order. This is the design to push
   the customer towards.
2. **Otherwise, scope the read per stream.** One writer per stream, all streams concurrent. Keeps
   V1's mtime-ordering logic including the same-mtime guard, which stays mandatory.

Cross-stream ordering is meaningless and is not attempted. This is the same argument V1 already
used to allow cross-project mtime ties.

> **Carried-over trap.** `os.utime` does not work on UC Volumes — it fails silently and the mtime
> stays the wall-clock write time. Any fixture generator that needs distinct mtimes must space its
> writes in real time. Volume mtime granularity is one second.

---

## Repository layout

```
databricks_V2/
├── docs/                     architecture.md (this), scaling-to-a-framework.md
├── framework/                importable modules — no notebook magic, unit-testable
│   ├── config.py             load layer 1a/1b/2, validate, resolve environment
│   ├── naming.py             stream → catalog.schema.table, the only place names are built
│   ├── discovery.py          list pending files, checksum, consult meta.ingested_file
│   ├── readers.py            per-format readers; parse mode by load mode
│   ├── hashing.py            norm / canon / _row_hash — pure, catalog-free
│   ├── mapping.py            load_release, raw_expr, typed_expr, build_select
│   ├── registry.py           schema_ver + drift policy
│   ├── quality.py            declarative DQ evaluation → _dq_status
│   └── historisation/        scd2.py, current_only.py, insert_only.py, append.py, snapshot.py
├── jobs/                     thin notebooks — orchestration only, no logic
│   ├── 00_setup.py           catalogs, schemas, volumes, meta DDL
│   ├── 01_deploy_config.py   YAML/CSV → meta.*, bitemporal
│   ├── 02_ingest.py          one stream → bronze
│   ├── 03_build_silver.py    one stream → silver
│   ├── 04_build_transform.py work + gold
│   └── 90_rebuild_silver.py  full rebuild + reconciliation
├── sources/                  layer 1a — entity YAML
├── mappings/                 layer 1b — column CSV
├── environments/             layer 2 — dev/test/prod YAML
├── transform/                gold/, work/, macros/, models.yml  (scaffolding this pass)
├── fixtures/                 generated test data
├── tests/                    pytest against local Spark
├── resources/                bundle job + pipeline definitions
├── explore/                  scratch notebooks, never referenced by jobs
└── spike_auto_cdc/           closed spike, kept runnable
```

The split that matters: **`framework/` holds logic and is unit-testable without a workspace;
`jobs/` holds notebooks that only parameterise and call it.** Both August 2026 bugs were in pure
functions (`canon`, and the ordering window) and would have been caught by tests on `framework/`.

---

## Orchestration

One job per layer, fanning out over streams read from `meta.stream` — Databricks' own
[control table driving a `For each` job](https://learn.microsoft.com/azure/databricks/jobs/how-to/foreach-sql-lookup-tutorial)
pattern, *"the data, not the code, controls what the job processes"*.

```
01_deploy_config
       │
       ▼
   For each stream ──► 02_ingest ──► 03_build_silver
       │
       ▼
04_build_transform   (work, then gold)
```

- Ingest and silver fan out per stream and run concurrently — one writer per stream, no shared
  sequence, so no contention.
- Transform runs once, after all streams, because it may join across them.
- Triggered, not continuous. Databricks' guidance is that the vast majority of pipelines should be
  triggered.
- Production jobs run as **service principals**, not a user identity.

Environments map onto bundle `targets` in `databricks.yml`. Note that *Databricks Asset Bundles*
have been renamed *Declarative Automation Bundles*; the CLI verbs are unchanged.

---

## Historisation policies

`build_silver` dispatches on the entity's declared policy. Everything upstream — staging, mapping,
hashing, deletion detection — is shared, and only the final step differs.

| Policy | Silver shape | Deletes | Incremental? |
|---|---|---|---|
| `scd2` | `version_no`, `valid_from`/`valid_to`, `is_current` | tombstone version | hard — full rebuild for now |
| `current_only` (Type 1) | one row per `entity_key` | DELETE or soft flag | **yes, cheap** |
| `insert_only` (Type 0) | append, first write wins | ignored | yes |
| `append` | no key, no dedup | n/a | yes |
| `snapshot` | partitioned by delivery date | implicit | yes |

Two observations worth recording:

- **Type 1 is a strict simplification of SCD2** — take the last row per key by `_ingest_seq`.
  Identical staging and mapping; only the final window changes.
- **The no-op collapse stops mattering under Type 1.** Resent unchanged rows are harmless, and the
  `_row_hash` anti-join survives purely as a cost optimisation. The `StructType.add` bug was
  SCD2-specific *in its consequences* — under Type 1 it would have been invisible.

---

## Data quality

V1 hardcodes `level or object_id is null` inside `silver_builder`. V2 evaluates `meta.dq_rule` per
entity: required columns, value domains, thresholds, with `quarantine` / `drop` / `fail` behaviour
declared per entity.

Quarantined rows are written, flagged `_dq_status`, and excluded by the gold views — never dropped,
because bronze must stay reproducible into silver.

---

## Testing

Non-negotiable for V2, because the failure modes seen so far are silent — a green run producing a
plausible-looking table.

| Level | Target | Runs on |
|---|---|---|
| Unit | `framework/` pure functions: `canon`, hashing, `raw_expr`, each historisation policy | local Spark, CI, no workspace |
| Fixture | generated deliveries covering resend / change / delete / schema drift | local Spark |
| Reconciliation | rebuild vs incremental; V2 output vs V1 baseline for AAS Doors | workspace |

The third is available for free: [91_verify_stage2.py](../spike_auto_cdc/explorations/91_verify_stage2.py)
already compares two historisation implementations on entity sets, version counts, current state
and version boundaries. It was written for the spike and is retained as a standing regression
harness — V2's AAS Doors output must reproduce V1's `silver.entities` exactly.

**Row-count reconciliation must be an automated assertion, not a manual check.** The AUTO CDC spike
produced 431 versions where 274 were expected, and the pipeline reported success; only an assertion
caught it.

---

## Deliberately deferred

| Item | Why not now |
|---|---|
| Incremental SCD2 merge | V1's full rebuild is correct; incremental is an optimisation with a correctness cliff. Build the reconciliation harness first |
| Blue/green swap | Design only, per instruction 2026-08-14 |
| Out-of-order arrival within a stream | Detect and quarantine; rebuild that stream. Routine tooling only if the customer confirms it happens |
| Streaming / near-real-time | No SLA established. Would change the ingest design materially |
| Star schema in gold | Depends on whether consumers need point-in-time joins across entities |
| `transform/` beyond scaffolding | Gold shape follows consumer requirements, which are not gathered |

---

## Open questions that block parts of this design

Carried from [scaling-to-a-framework.md §6](scaling-to-a-framework.md). Listed here because each
one blocks a specific decision above, rather than being general unknowns.

| Question | Blocks |
|---|---|
| Do delta files carry **complete rows**? | Whether hashing can compare rows directly, or must merge onto the last version first |
| Can sources provide **manifests** (row count, sequence number, checksum)? | The derived `_ingest_seq` design; the truncated-full-load guard |
| Can a full load arrive **truncated**? | Whether tombstone minting needs a row-count floor |
| Can extracts arrive **out of order** within a stream? | Whether late arrival is routine tooling or break-glass |
| Does the **per-tenant column renaming** pattern recur beyond AAS Doors? | Whether the catalog machinery is core or an AAS Doors quirk |
| Who maintains the mappings — engineers or analysts? | YAML-in-git versus a UI. Hard to reverse |
| **Erasure** requirements? | Whether immutable bronze survives contact with GDPR |
| Per-project **access isolation**? | Whether silver is shared or per-tenant |

The first two are the highest value. A manifest with a sequence number retires an entire class of
ordering bugs — including the one that cost a debugging cycle on 2026-08-14 — and unlocks the
best available `_ingest_seq` design.
