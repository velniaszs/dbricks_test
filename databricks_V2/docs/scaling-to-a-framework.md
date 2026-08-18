# Scaling the PoC into a reusable DWH framework

Status: **thinking document, nothing here is built.** It records the analysis of what would have to
change to turn the AAS Doors PoC into something the customer can point at a second, third and
twentieth source. `../../databricks_v1/docs/architecture.md` describes what exists; this describes
what it would take to generalise it, and deliberately stops short of prescribing an implementation
order beyond the few items that are expensive to retrofit.

**The design that came out of this analysis is [architecture.md](architecture.md).** Read that for
what V2 *is*; read this for why.

Written 2026-08-14, off the back of the first end-to-end PoC run.

Section 7 was added the same day after checking the design against current Azure Databricks
guidance. It changes two conclusions in the earlier sections, which are cross-referenced where
they occur. One open question was settled by a timeboxed spike in
`databricks_V2/spike_auto_cdc/` — see the outcome note at the end of §7.2.

---

## 1. The core diagnosis

The PoC has exactly one logical entity -- AAS Doors requirement objects -- and one axis of
variation, `project_id`. A framework needs three axes:

| Axis | Meaning | PoC status |
| --- | --- | --- |
| **Source system** | AAS Doors, SAP, Teamcenter -- different delivery mechanics, formats, contracts | hardcoded |
| **Entity / table** | requirement, change request, part, BOM line -- different schema, key, historisation | hardcoded |
| **Tenant / variant** | FERRARI, MCLAREN -- *same* entity, different column spellings | solved well |

The encouraging part is that the hardest and rarest axis is the one already solved. Per-tenant
column renaming resolved through a bitemporal catalog is the thing most frameworks cannot do.
Axes 1 and 2 are the well-trodden ones.

The catalog machinery also generalises directly. Today it maps
`(project_id, source_column) -> generic_column`. Widen the scope key to
`(source_system, entity, project_id)` and the same `raw_expr` CASE-arm mechanism covers all three
axes with no new concepts.

**Verdict: this is a re-scoping, not a rewrite.** Three things are scoped to "one entity" and need
one more level of key; one thing needs to become pluggable. Everything else holds.

What is scoped too narrowly:

1. `_ingest_seq` is global across all of bronze -- should be per stream.
2. Table names are module constants -- should be resolved from metadata.
3. `GENERIC_COLUMNS` lives in Python -- should be a `meta` table.

What needs to become pluggable:

4. `build_silver` always does SCD2 -- should dispatch on a declared historisation policy.

---

## 2. What breaks when many tables arrive

### 2.1 The global `_ingest_seq` is the real blocker

`architecture.md` justifies "one ingest job, one writer, all projects" on the grounds that
concurrent MERGEs into silver conflict. That was correct for one entity. With 20 projects and
50 tables it means serialising every ingest in the platform through a single job.

But the reason it is single-writer is this line in `02_bronze_ingest`:

```python
base_seq = spark.sql(f"SELECT coalesce(max(_ingest_seq), 0) ...").collect()[0]["s"]
```

The constraint comes from deriving the sequence by reading `max()` -- a read-modify-write. It is
**not** fundamental. The actual job of `_ingest_seq` is "a total, reproducible order *within one
entity's timeline*". Entities never span streams, so ordering across streams is meaningless. That
is precisely the argument already used to allow cross-project mtime ties in `list_landing_files()`.

Two consequences:

- **Rescope to per stream**, where a stream is `(source_system, entity)`. One writer per stream,
  streams run fully in parallel. `meta.project_state` becomes `meta.stream_state`.
- **Better still, derive the sequence rather than read it.** If a delivery carried a manifest
  sequence number, `_ingest_seq = delivery_seq * 10^9 + row_num` needs no read at all. That removes
  writer serialisation *and* makes rebuilds stable even when bronze is reprocessed out of order.

This is the highest-leverage single change on the list.

### 2.2 Bronze table granularity

Recommendation: **one bronze table per stream**, not one global table. Independent
OPTIMIZE/VACUUM/retention, bounded blast radius, parallel writers, per-entity retention tiers.
Unity Catalog handles thousands of tables without complaint.

Bronze's VARIANT payload is what makes this cheap. The ingest code is already schema-agnostic, so
per-stream tables need no per-stream code -- only a metadata lookup for the target table name.

The alternative -- one bronze table with an `entity_name` column -- is tempting for code
simplicity, but it forces every stream through one writer for sequence assignment and one
OPTIMIZE/VACUUM schedule. Only worth revisiting if the number of genuinely tiny entities makes
table sprawl the dominant cost.

### 2.3 Onboarding cost per table

Adding a project today means editing `poc_config.py` and re-running `03_seed_metadata`. At 500
stream x tenant combinations that is untenable. What must move from code into `meta` tables:

- `GENERIC_COLUMNS` -> `meta.entity_column` (the target schema per entity)
- entity definitions: business key, historisation policy, landing contract, source format
- DQ rules per entity
- schema-evolution policy per entity

**Watch the trap.** "Config as data" on its own gives untraceable production changes and no
dev -> prod promotion path. Delta tables are not git. The reconciliation: keep the *declarations*
as YAML in the repository, and have a deploy step load them into the `meta` tables bitemporally,
preserving `recorded_at` / `superseded_at`. That gives review, history and reproducible rebuilds,
and leaves the door open to generating the YAML from a UI later for non-engineers.

### 2.4 Ingest idempotency does not scale

`already_ingested` is a `SELECT DISTINCT _source_file` over the whole of bronze -- a growing full
scan every run. It also silently mishandles a corrected file redelivered under the same name.

Should become a `meta.ingested_file` ledger keyed by path plus content checksum, recording
`ingested_at` and `batch_id`. Cheap, exact, and it makes "same name, new content" a supported case
rather than a silent data-loss bug.

### 2.5 File discovery

`dbutils.fs.ls` is O(n) per run and slow at thousands of files. Autoloader with file notifications
is the scaling answer. Note that Autoloader provides no total order, which pushes the design
towards manifest-derived sequence numbers again (see 2.1 and 4.3).

### 2.6 Orchestration and cost

Notebooks currently run in sequence by hand. N streams need a driver: a jobs-API loop or DLT, with
per-stream tasks, retries and a control table. One job per stream means many cluster starts --
either batch streams into a single looping job, or lean on serverless.

---

## 3. Entities that are not SCD2

### 3.1 Terminology

"Just show what is currently active" is **Type 1** -- overwrite, no history. **Type 0** means
"never update after insert", used for immutable reference data. The distinction matters because
both are worth supporting and they behave differently on redelivery.

### 3.2 Policies worth supporting

| Policy | Use | Silver shape |
| --- | --- | --- |
| `scd2` | today's `silver.entities` | `version_no`, `valid_from` / `valid_to`, `is_current` |
| `current_only` (Type 1) | status lookups, config tables | one row per `entity_key` |
| `insert_only` (Type 0) | immutable reference data | append, first write wins |
| `append` | events, measurements, facts | no key, no dedup, no versioning |
| `snapshot` | when "as of delivery" beats interval logic | partitioned by delivery date |

### 3.3 Does the architecture hold? Yes, and unusually cleanly

> **Settled 2026-08-18.** `AUTO CDC` was spiked and works (see §7.2), but **this version keeps the
> custom `assign_versions`** — the deciding factor is explicit control over what gets merged, not
> capability. So the policy table above stays a real dispatch table rather than collapsing into a
> `stored_as_scd_type` argument, and the reasoning below applies as written.

`assign_versions` is a pure function over bronze order, and Type 1 is a strict *simplification* of
it: take the last row per `entity_key` by `_ingest_seq`. Identical staging, mapping, hashing and
deletion detection; only the final window differs. `build_silver` dispatches on a `historisation`
property and everything upstream is shared.

Three things to think through:

- **Deletes.** `deletion_events` is still needed to know an entity vanished, but for Type 1 it
  triggers a DELETE or sets a soft-delete flag rather than minting a tombstone. Hard delete is
  acceptable -- bronze remains the source of truth, so a full rebuild still reproduces the table.
- **Type 1 can MERGE incrementally** rather than rebuild, because there is no timeline to keep
  internally consistent. Substantially cheaper. This is where the incremental path is genuinely
  easy, in contrast to SCD2.
- **The no-op collapse stops mattering.** Resent unchanged rows are harmless under Type 1; the
  `_row_hash` anti-join survives purely as a cost optimisation. Worth recording that the
  `StructType.add` bug found on 2026-08-14 was SCD2-specific *in its consequences* -- under Type 1
  it would have been invisible.

---

## 4. Gaps not yet hit

### 4.1 Point-in-time joins across entities

The thing that most reliably surprises teams scaling SCD2. Once there are relationships --
requirement to parent requirement, part to BOM line -- "what did this look like on 2026-03-01"
becomes a multi-table interval join.

Decide early whether gold is entity timelines plus PIT helper views, or a dimensional model where
facts carry a surrogate key pointing at a *specific version* of a dimension. If the latter, a
per-version surrogate is required; `entity_key || version_no` suffices, but retrofitting it after
history accumulates is painful.

### 4.2 Declarative DQ rules

`_dq_status` currently hardcodes "level or object_id is null" inside `silver_builder`. Every new
entity would mean editing shared code. Needs required-columns, value domains and thresholds
declared per entity in metadata.

### 4.3 Schema evolution policy, not just detection

`meta.schema_registry` detects drift but nothing decides what to do about it. A new column staying
in `payload` is harmless and should auto-accept. A business-key column disappearing must fail
loudly and stop the stream. That policy belongs per entity, alongside the entity definition.

### 4.4 Delivery manifests

Worth pushing for with every new source: row count, extract timestamp, sequence number, checksum.

- Row count is the direct answer to the still-open "can a full load arrive truncated?" question --
  the one where a wrong answer causes mass false tombstones.
- A sequence number retires the entire class of ordering bugs that cost a debugging cycle on
  2026-08-14, and unlocks the derived-`_ingest_seq` design in 2.1.

### 4.5 Run log and observability

`meta.run_log`, one row per stream per run: files seen, rows in, versions minted, no-op ratio,
cast failures, duration, cost. The no-op ratio is already identified in `architecture.md` as the
two-sided health signal for delta deliveries -- it needs to be recorded systematically before
there are 500 streams and no way to tell which one degraded.

### 4.6 Testing

Both bugs found on 2026-08-14 -- `StructType.add()` mutating in place, and mtime ties inverting
the version chain -- were catchable by unit tests on pure functions. `canon`, `assign_versions`
and `raw_expr` all run against a local Spark session with no Databricks workspace.

The generator plus the assertions in `04_build_silver` already constitute an integration suite.
A framework needs them running in CI, not as cells someone remembers to execute.

### 4.7 GDPR versus immutable bronze

A genuine tension: right-to-erasure against "bronze is the immutable source of truth and rebuilds
are deterministic". Deletion vectors make the delete itself cheap, but once a row is erased from
bronze a rebuild no longer reproduces prior silver. Needs an explicit decision, and it is far
easier to make now than after three years of history.

### 4.8 Retention

Ten years of bronze at roughly 137M rows per three years for *one* entity. Multiply by 50 entities
and the 100 GB estimate stops being comfortable. Needs per-entity retention tiers, and a decision
on whether old bronze can be archived once silver is treated as authoritative -- which trades away
full-rebuild capability for that period.

### 4.9 Multi-tenancy and isolation

Twenty projects today. If this becomes a platform, who may see what? Unity Catalog supports
per-schema grants, and row filters or column masks if one silver table serves many projects. Worth
deciding early because it determines whether silver is per-project or shared.

---

## 5. Advice on sequencing

**Do not build the framework speculatively. Onboard a second source system first, then generalise.**

The risk of generalising now is building for the wrong axis. The per-tenant column-renaming
machinery is the most expensive thing in the codebase, and it may well be an AAS Doors quirk. If
the second source has one schema and no tenants, that investment was optimising for a rarity.

That said, four items are expensive to retrofit and worth doing before the second source arrives:

1. Rescope `_ingest_seq` to per stream and stop deriving it from `max()`.
2. Move `GENERIC_COLUMNS` and the entity definition out of Python into `meta`, fed by git-backed
   YAML.
3. Add the `historisation` property and the Type 1 branch -- cheap now, and it validates the
   abstraction before it is load-bearing.
4. Replace the `_source_file` scan with `meta.ingested_file`.

---

## 6. Open questions for the customer

**Scope**

- How many source systems, entities and tenants realistically -- 5 tables or 500?
- Does the per-tenant column-renaming pattern recur, or is it specific to AAS Doors?

**Ownership**

- Who maintains the mappings: engineers or business analysts? This decides YAML-in-git versus a
  UI, and the decision is hard to reverse.

**Per entity**

- Which entities genuinely need history, and which are current-state only?
- What is the SLA per entity -- daily, hourly, near-real-time? A streaming requirement would
  change the ingest design materially.

**Sources**

- Can they provide manifests with row counts and sequence numbers?
- Formats beyond CSV? Anything push-based or API-based rather than file drops?

**Modelling**

- Will consumers need to join entities as of a point in time?
- Is a star schema the eventual target, or are entity timelines consumed directly?

**Governance**

- Erasure requirements?
- Per-project access isolation -- may all 20 projects see each other's data?

**Still outstanding from the PoC, now more urgent because it shapes every future source**

- Do delta files carry complete rows, or only the changed fields?

---

## 7. Verification against Databricks guidance

Checked 2026-08-14 against current Azure Databricks documentation. Most of the design is confirmed.
Three findings are material, and one of them changes what gets built.

### 7.1 Confirmed

| Design choice | Guidance |
| --- | --- |
| Bronze as `STRING` + `VARIANT` payload | *"store most fields as string, VARIANT, or binary to protect against unexpected schema changes"* -- [medallion](https://learn.microsoft.com/azure/databricks/lakehouse/medallion) |
| Bronze not consumer-facing | *"intended for consumption by workloads that enrich data for silver tables, not for access by analysts"* |
| Bronze is the rebuild source | *"single source of truth... Enables reprocessing and auditing by retaining all historical data"* |
| Gold holds only consumer-facing objects | *"designed for business users... contains fewer datasets than silver and bronze"* |
| SCD2 history lives in silver, not gold | *"Large amounts of historical data are typically accessed in the silver layer and not materialized in the gold layer"* |
| Catalog per environment | *"catalogs correspond to an environment scope, team, business unit, or some combination"* -- [UC best practices](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/best-practices) |
| Config as data, driving job fan-out | Documented pattern, named as such: [Use a control table to drive a For each job](https://learn.microsoft.com/azure/databricks/jobs/how-to/foreach-sql-lookup-tutorial) -- *"the data, not the code, controls what the job processes"* |
| Catalog/schema as parameters, never literals | *"avoid hardcoding those names... define them as pipeline configuration parameters"* |
| Quarantine rather than drop bad rows | Documented quarantine pattern; matches `_dq_status = 'quarantine'` |

### 7.2 Databricks implements SCD1 and SCD2 natively, and recommends it over custom logic

From [Best practices for Lakeflow pipelines](https://learn.microsoft.com/azure/databricks/ldp/best-practices):

> **Use declarative CDC instead of imperative MERGE.** Implementing change data capture with
> imperative SQL `MERGE` statements requires significant custom code to handle event ordering,
> deduplication, partial updates, and schema evolution correctly. Each of these concerns must be
> solved independently, and the resulting code is difficult to maintain and test.

`AUTO CDC ... INTO` (formerly `APPLY CHANGES INTO`) handles ordering, out-of-order events,
deduplication and the no-op collapse, and writes `__START_AT` / `__END_AT`. That is
`assign_versions` as a platform feature. It also offers `TRACK HISTORY ON ... EXCEPT` for
column-subset versioning, per-run `num_upserted_rows` / `num_deleted_rows` metrics, and bitemporal
tracking (Beta).

This is a direct description of the code that produced both bugs found on 2026-08-13 and
2026-08-14. Both lived in the version-assignment path.

**One shape is ruled out by documentation, without needing a test.** From
[create_auto_cdc_from_snapshot_flow](https://learn.microsoft.com/azure/databricks/ldp/developer/ldp-python-ref-apply-changes-from-snapshot):

> You cannot target the same streaming table with both `create_auto_cdc_from_snapshot_flow()` and
> `create_auto_cdc_flow()`.

Bosch delivers weekly full snapshots *and* daily partial deltas for the same entity, so neither API
covers the stream alone and they cannot be combined on one target.

**The viable shape** keeps the easy half and delegates the hard half:

```
bronze.events  --batch: UPSERT per row, DELETE derived from full-load absence-->
bronze.change_feed  --create_auto_cdc_flow(scd_type=2)-->  silver.entity_history
```

`deletion_events` survives as a set difference over bronze, which is cheap and was never the
problem. `assign_versions` disappears.

Being tested in `databricks_V2/spike_auto_cdc/`, in two stages. Stage 1 is synthetic and answers
whether the semantics work at all: unchanged resends collapse, absence tombstones correctly, full
and delta interleave in one chain, and a full refresh reproduces an identical table. Stage 2 runs
the real bronze table through the real column catalog and compares the result row for row against
V1's `silver.entities`; it reuses `load_release`, `build_select` and `deletion_events` unchanged so
that any difference is attributable to `AUTO CDC` alone.

The deterministic-rebuild check is the one that matters, because rebuild-from-bronze is the reason
bronze exists.

One expected structural difference: V1 stores a tombstone **row** per deletion, `AUTO CDC` closes
the previous version and stores nothing. Adopting it means `_change_reason` and the tombstone
become a view over `__END_AT` rather than stored data.

Residual risks recorded there: the sequencing column must be monotonic with one distinct update
per key per value; delete emission must be idempotent; `__START_AT` / `__END_AT` are
platform-named, so `valid_from` / `valid_to` / `is_current` become a view.

#### Outcome — spiked, works, not adopted for this version

**Stage 1 passed in full**, including the deterministic-rebuild fingerprint across a
`Full refresh all`. Stage 2 initially reported 431 versions against an expected 274, but that was
a harness defect rather than a platform one: `except_column_list` governs only which columns are
*written*, while version boundaries compare every remaining output column — so the per-delivery
provenance columns made every resend look like a change. The correct parameter is
`track_history_except_column_list`.

**Decision (2026-08-18): keep the custom historisation for this version.** The argument for
`AUTO CDC` was never in doubt on capability; the argument against is control. Version assignment
is the most consequential step in the pipeline, it is where both 2026-08 bugs lived, and an
explicit merge is one that can be read, unit-tested against a fixture, and stepped through when a
customer disputes a version boundary. Delegating it trades that for a platform behaviour whose
comparison semantics are configured through two similarly-named parameters — exactly the
distinction that produced the 431/274 discrepancy above.

What the spike bought regardless of the decision:

- Confirmation that the change-feed shape (`UPSERT` per row, `DELETE` derived from full-load
  absence) is sound independently of who consumes it. That normalisation stays.
- A reconciliation harness in `databricks_V2/spike_auto_cdc/explorations/91_verify_stage2.py` that
  compares any two historisation implementations on entity sets, version counts, current state and
  version boundaries. Reusable as a standing regression check.
- The knowledge that this failure mode is **silent**: the pipeline ran green and produced a
  plausible SCD2 table. Whatever implements versioning, row-count reconciliation has to be an
  automated assertion, not a manual spot-check.

**Revisit when** the incremental SCD2 merge is due to be built, or if `assign_versions` accrues a
third correctness bug. The spike is left in place and runnable so the decision can be re-tested
rather than re-argued.

### 7.3 Silver may legitimately contain joins

The medallion documentation lists **Joins** among silver operations, and its worked example joins
customers and transactions into `customer_transactions` *in silver*. Placing intermediate,
enriched datasets in silver is the documented pattern, not a compromise.

This overrides the earlier working rule that silver must be strictly single-entity and mechanical.
Silver gets two zones instead:

- **Framework-built silver** -- entity-faithful, one row per source record, written only by the
  framework. Databricks requires this regardless: *"Should always include at least one validated,
  non-aggregated representation of each record."*
- **Derived silver** -- joined and enriched datasets built from the above by authored SQL.

The rebuild guarantee survives, because `bronze -> silver -> derived silver -> gold` is still a
deterministic chain. What the earlier rule was actually protecting against was an overwriting
framework build clobbering hand-authored tables -- an implementation concern, solved by having the
framework own only the tables it declares.

Gold remains consumer-facing only. That part was right.

### 7.4 Landing volumes should be external, not managed

> *"Use external volumes: To register landing areas for raw data produced by external systems to
> support its processing in the early stages of ETL pipelines."*

The PoC uses a managed volume. Acceptable for a PoC, wrong for production where files are dropped
from outside Databricks.

Related, and relevant to §2.5: enabling **file events** on the external location *"improves the
performance and reliability of downstream features, such as file arrival triggers and Auto
Loader."* That is the answer to `dbutils.fs.ls` not scaling, and it allows ingestion to trigger on
arrival rather than on a schedule.

### 7.5 Smaller corrections

- **Databricks Asset Bundles are now Declarative Automation Bundles.** Same tool, renamed. Still
  the recommended CI/CD mechanism, still YAML with per-target overrides -- the environment config
  layer maps onto `targets` directly.
- **Gold aggregates should be materialized views**, not plain views. Incremental refresh partly
  answers the problem of a silver rebuild invalidating materialised gold.
- **Temporary views** are the platform's name for "intermediate step that materialises nothing",
  if the transformation layer is built on pipelines rather than a separate runner.
- **Production jobs should run as service principals**: *"If you use users to run jobs that write
  into production, you risk overwriting production data by accident."* Not currently in the design.
- **Expectations** (`warn` / `drop` / `fail`) cover most of what `_dq_status` does by hand, and
  emit quality metrics to the pipeline event log for free.

### 7.6 Not yet verified

- Recursive CTE support on the target runtime, referenced in §4.1.
- Whether a `For each` task wrapping a *pipeline* task is workable at 20+ streams. Documented cap:
  *"A pipeline task wrapped in a for-each task is capped to one concurrent iteration, regardless of
  the loop's configured concurrency."* This is a real constraint on the fan-out design if every
  stream becomes its own pipeline.
