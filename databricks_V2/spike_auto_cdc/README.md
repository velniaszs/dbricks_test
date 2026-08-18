# Spike: can AUTO CDC replace the hand-rolled historisation?

Timeboxed spike. Nothing here is framework code. Decide, then delete or promote.

## Why

> **Outcome, 2026-08-18: spiked, works, not adopted.** Stage 1 passed in full including the
> deterministic-rebuild fingerprint. Stage 2's first failure (431 versions vs 274 expected) was a
> harness defect in this folder, not a platform one — see the first caveat below. **The decision is
> to keep the custom `assign_versions` for this version**, on grounds of explicit control over the
> merge rather than capability. This folder stays runnable so the decision can be re-tested rather
> than re-argued; revisit when the incremental SCD2 merge is built. Full reasoning in
> `databricks_V2/docs/scaling-to-a-framework.md` §7.2.

`silver_builder.assign_versions()` implements SCD2 with window functions: `lag` to collapse
no-op resends, `lead` to close `valid_to`, plus a separate `deletion_events()` pass to
tombstone entities that vanish from a full load. Both bugs found on 2026-08-13/14 lived in
exactly this code.

Databricks' published position ([Best practices for Lakeflow pipelines](https://learn.microsoft.com/azure/databricks/ldp/best-practices)):

> **Use declarative CDC instead of imperative MERGE.** Implementing change data capture with
> imperative SQL `MERGE` statements requires significant custom code to handle event ordering,
> deduplication, partial updates, and schema evolution correctly... the resulting code is
> difficult to maintain and test.

If `AUTO CDC` holds up, the riskiest third of the V2 framework never gets written.

## What the docs already settled — do not test these

| Question | Answer | Source |
| --- | --- | --- |
| Can a snapshot flow and a CDC flow write to one target? | **No.** *"You cannot target the same streaming table with both `create_auto_cdc_from_snapshot_flow()` and `create_auto_cdc_flow()`."* | [create_auto_cdc_from_snapshot_flow](https://learn.microsoft.com/azure/databricks/ldp/developer/ldp-python-ref-apply-changes-from-snapshot) |
| Is `AUTO CDC FROM SNAPSHOT` available in SQL? | No, Python only | [AUTO CDC APIs](https://learn.microsoft.com/azure/databricks/ldp/cdc) |
| Does it need a specific pipeline edition? | Serverless, or Pro/Advanced | same |
| Can history track a column subset? | Yes, `track_history_except_column_list` | same |

That first row kills the obvious design. Bosch delivers **weekly full snapshots *and* daily
partial deltas into the same entity**, so neither API alone covers the stream.

## The design under test

Keep the easy half (set difference), delegate the hard half (versioning):

```
bronze.events              append-only, one row per source row, existing 02_bronze_ingest
      |
      |  batch: emit UPSERT for every row; derive DELETE for keys known before a
      |         full load but absent from it  <- this is today's deletion_events(), kept
      v
bronze.change_feed         append-only, columns: op, seq, + payload
      |
      |  Lakeflow pipeline: create_auto_cdc_flow(stored_as_scd_type=2)
      v
silver.entity_history      __START_AT / __END_AT maintained by Databricks
```

`assign_versions()` disappears. `deletion_events()` survives as a set difference over bronze,
which is cheap and was never the buggy part.

## Fixture

Five entities, three extracts, chosen so every semantic we care about is exercised:

| seq | mode | contents |
| --- | --- | --- |
| 1 | full | A, B, C, D |
| 2 | delta | B *changed*, C *resent unchanged*, E *new* |
| 3 | full | A, B *changed again*, C, E — **D absent** |

## Two stages

**Stage 1** — synthetic, 12 rows, own tables. Answers *do the SCD2 semantics work at all?*
Deliberately avoids `VARIANT`, the column catalog and hashed keys so a failure is unambiguous.

**Stage 2** — the real `bronze.aas_doors_raw`, the real catalog, three projects, real
`entity_key` hashes, compared row for row against V1's `silver.entities`. Answers *does it
reproduce what we already built?*

Stage 2 imports `silver_builder` from `databricks_v1/` **read-only** and reuses `load_release`,
`build_select` and `deletion_events` unchanged. Only `assign_versions` is replaced. That is the
point: any difference in the output is attributable to `AUTO CDC` and not to a reimplementation.
Nothing under `databricks_v1/` is written to.

## Pass / fail criteria

### Stage 1 — `90_verify.py`

| # | Assertion | What it proves |
| --- | --- | --- |
| Q1 | C has exactly **one** version | No-op resends collapse — the 2026-08-14 bug, handled natively |
| Q2 | D has one version, `__END_AT` set, and is **not** current | Deletion-by-absence closes the chain |
| Q3 | B has **three** versions in seq order 1 → 2 → 3 | Full and delta interleave correctly in one feed |
| Q4 | 7 rows total, 4 current (A, B, C, E) | Whole-table shape |
| Q5 | Full refresh reproduces byte-identical history | Deterministic rebuild from bronze survives |

Q5 is the one that matters most for us, because "rebuild silver from bronze" is a hard
requirement and the whole reason bronze exists.

### Stage 2 — `91_verify_stage2.py`

| # | Assertion | What it proves |
| --- | --- | --- |
| R1 | Same `(project_id, entity_key)` set | Mapping and hashing survived the new path |
| R2 | `cdc_rows == v1_rows - v1_tombstones` | Version totals reconcile |
| R3 | Per-entity version counts match | Collapse behaves identically on real data |
| R4 | Current state is row-for-row identical | What a consumer actually reads is unchanged |
| R5 | Deleted entities have no current row | The `is_current` contract holds |
| R6 | Version boundaries land on the same `_ingest_seq` | Ordering agrees, not just counts |

R2 encodes a real and expected difference: V1 writes a tombstone **row** per deletion
(`_change_reason = 'deleted'`), `AUTO CDC` closes the previous version and writes nothing. So
AUTO CDC has exactly one fewer row per deletion. If we adopt this, `_change_reason` and the
tombstone row become a view over `__END_AT`, not stored data.

## How to run

### What to import, and what each file is

All six `.py` files start with `# Databricks notebook source`, so all six import as **notebooks**.
That is not the same as all six being *run* as notebooks:

| File | Import as | How it is used |
| --- | --- | --- |
| `explorations/00_setup_fixtures.py` | Notebook | Run it — stage 1 |
| `transformations/entity_history.py` | Notebook | **Pipeline source file** — never "Run all" |
| `explorations/90_verify.py` | Notebook | Run it — stage 1 |
| `explorations/10_stage2_change_feed.py` | Notebook | Run it — stage 2 |
| `transformations/entities_real.py` | Notebook | **Pipeline source file** — never "Run all" |
| `explorations/91_verify_stage2.py` | Notebook | Run it — stage 2 |

The two `transformations/` files call `dp.create_auto_cdc_flow`, which only exists inside a
pipeline run. Opening one and pressing Run all fails on the import; that is expected.

**Stage 1 is self-contained** — the two files import nothing from this repo, so they can go in any
workspace folder.

**Stage 2 imports V1**, and resolves it by relative path:

```python
V1_PATH = os.path.normpath(os.path.join(_here, "..", "..", "..", "databricks"))
```

So the repo nesting has to survive the import:

```
<any folder>/
  databricks_v1/                  <- poc_config.py, silver_builder.py as workspace FILES
  databricks_V2/spike_auto_cdc/
    explorations/
    transformations/
```

Cloning the repo as a Git folder gives you this for free and is the least error-prone route. If
you import manually instead, recreate the nesting or edit the `V1_PATH` line — there is an assert
with the resolved path in the message if it misses.

### Stage 1

1. `explorations/00_setup_fixtures.py` as a normal notebook. Creates `bosch_poc.spike`,
   the bronze fixture, and the change feed.
2. Create a Lakeflow pipeline:
   - source file: `transformations/entity_history.py`
   - default catalog `bosch_poc`, default schema `spike`
   - serverless, **triggered** mode
   - Run it.
3. `explorations/90_verify.py` as a normal notebook.
4. For Q5: **Full refresh all** on the pipeline, then re-run `90_verify.py`.

### Stage 2

Only worth doing if stage 1 passes.

0. **Prerequisite: a clean bronze.** Re-run `databricks_v1/` `00 -> 02 -> 04` after the
   `StructType.add` fix, otherwise `_row_hash` still carries the bug and the collapse cannot be
   judged. V1 silver must be built from the same bronze contents, or R2–R6 compare two different
   worlds.
1. `explorations/10_stage2_change_feed.py`. Check `V1_PATH` resolved — it assumes
   `databricks_v1/` and `databricks_V2/` are siblings in the workspace.
2. A second Lakeflow pipeline on `transformations/entities_real.py`, same settings.
   Use **Full refresh all**, since the feed table is rebuilt with `overwrite`.
3. `explorations/91_verify_stage2.py`.

## Known caveats to watch for, not blockers

- **`except_column_list` is not the version-comparison list.** It only controls which columns get
  *written*. Version boundaries are decided by comparing every remaining output column, so any
  per-delivery provenance left in the feed (`_ingest_seq`, `_source_file`, `_load_mode`, …) makes
  every resend look like a change and collapse stops entirely. Use
  `track_history_except_column_list` for that. Hit on the first stage 2 run: 431 versions instead
  of 274, i.e. one per feed row.
- **`_row_hash` must stay *tracked*.** It is what reproduces V1's rule that a change to *any*
  source column mints a version — including columns that were never promoted out of `payload`.
  Track only the promoted columns and a payload-only change (ALPINE's `Safety_Class_Alp`) would
  silently stop producing versions.

- **Sequencing contract.** `AUTO CDC` requires the sequencing column to be monotonic with
  *one distinct update per key per value*. Stage 1's `extract_seq` satisfies this only because a
  key appears at most once per extract; stage 2 uses `_ingest_seq`, which is unique per bronze
  row, and asserts the contract explicitly. Real deliveries that repeat a key within one file
  would need `struct(_ingest_seq, _row_hash)`.
- **Repeated deletes.** Stage 1's delete derivation emits a `DELETE` for any key known before a
  full load and absent from it — including a key deleted several full loads ago. Harmless there
  (one deletion, last extract). Stage 2 uses V1's `deletion_events`, which is already correct on
  this point.
- **`__START_AT` / `__END_AT` naming** is fixed by the platform, and they carry the *sequence
  value*, not a timestamp. Our gold contract uses `valid_from` / `valid_to` / `is_current` as
  timestamps, so that becomes a view with a join back to `_ingest_ts`, not stored columns.
- **No `_row_hash` in versioning.** AUTO CDC compares column values directly. `_row_hash` survives
  only to mark tombstones and for ingest-side dedup.
- **No DQ step** on the AUTO CDC path yet, so stage 2 compares values without the
  `_dq_status <> 'quarantine'` filter. Expectations would cover this later.
