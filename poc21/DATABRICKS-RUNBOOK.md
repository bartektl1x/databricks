# POC 2.1 Databricks runbook

Use this checklist to deploy and execute the reference package without
changing its architecture.

## 1. Choose the isolated namespace

The distributed files use:

```python
CATALOG = "main"
SCHEMA = "demo"
```

Either retain those values or replace both constants consistently in every
Python file. The publication catalog and schema of both pipelines must match
the constants.

Do not rename the `poc21_` objects during initial validation. Keeping the
names unchanged makes the supplied assertions authoritative.

## 2. Upload notebook scripts

Upload these as Databricks notebooks or workspace Python files that can be
run outside a pipeline:

```text
00_setup_and_reset.py
01_seed_csv.py
04_assert_initial_state.py
05_update_dqx_valid_row.py
06_assert_update_propagation.py
07_delete_dqx_valid_row.py
08_assert_delete_propagation.py
09_show_evidence.py
```

The mutation notebooks must run on compute that supports DML against Unity
Catalog streaming tables.

## 3. Create Pipeline 1

Use:

```text
Source:             02_retention_pipeline.py
Publication mode:   Unity Catalog
Catalog:            same as CATALOG
Schema:             same as SCHEMA
Mode:               triggered
Dependency:         databricks-labs-dqx
```

Pipeline 1 owns:

```text
poc21_bronze_customers
poc21_dqx_customers_valid
poc21_dqx_customers_quarantine
```

Do not add active Auto-TTL.

## 4. Create Pipeline 2

Use:

```text
Source:             03_cdc_marker_and_gold_pipeline.py
Publication mode:   Unity Catalog
Catalog:            same as CATALOG
Schema:             same as SCHEMA
Mode:               triggered
```

Use a current Lakeflow environment that supports AUTO CDC and reading CDF
from the published Pipeline 1 streaming table.

Pipeline 2 owns:

```text
poc21_hub_customers
poc21_satellite_customers_scd2
poc21_customer_deletion_markers
poc21_satellite_customers_classified
poc21_gold_customers_current
```

## 5. Clean initial execution

Stop both pipelines, then execute exactly:

```text
1. 00_setup_and_reset.py
2. 01_seed_csv.py
3. FULL REFRESH Pipeline 1
4. FULL REFRESH Pipeline 2
5. 04_assert_initial_state.py
```

Gate:

```text
POC 2.1 INITIAL-STATE ASSERTIONS PASSED
```

Do not proceed unless the gate passes.

## 6. Controlled update

Execute:

```text
6. 05_update_dqx_valid_row.py
7. NORMAL INCREMENTAL update of Pipeline 2
8. 06_assert_update_propagation.py
```

Gate:

```text
POC 2.1 UPDATE-PROPAGATION ASSERTIONS PASSED
```

Record the printed UPDATE commit version in `VALIDATION.md`.

Do not run Pipeline 1 in this phase.

## 7. Controlled delete

Execute:

```text
9.  07_delete_dqx_valid_row.py
10. NORMAL INCREMENTAL update of Pipeline 2
11. 08_assert_delete_propagation.py
```

Gate:

```text
POC 2.1 DELETE-PROPAGATION ASSERTIONS PASSED
```

Record the printed DELETE commit version in `VALIDATION.md`.

Do not run Pipeline 1 in this phase.

## 8. Incremental convergence

Execute:

```text
12. NORMAL INCREMENTAL update of Pipeline 2 with no new input
13. 08_assert_delete_propagation.py again
```

The second assertion run must still report:

```text
Hub C002 rows:           1
Satellite history rows:  2
Satellite active rows:   0
Deletion markers:        1
Delete-classified rows:  1
Gold C002 rows:          0
```

This proves ordinary no-new-input convergence. It does not prove arbitrary
full-refresh reconstruction.

## 9. Capture evidence

Run:

```text
09_show_evidence.py
```

Save:

- initial, update, delete, and convergence assertion output;
- Pipeline 1 and Pipeline 2 successful update identifiers;
- UPDATE and DELETE Delta versions;
- Pipeline 2 event-log metrics where available;
- runtime/channel and pipeline settings;
- Hub, Satellite, marker, classified, and Gold output screenshots or exports.

Complete every checkbox in `VALIDATION.md`.

## Reset rules

If a mutation script partially succeeds and then a later assertion fails,
assume the table was already mutated. Do not blindly rerun the mutation.

For a clean restart:

```text
1. Stop both pipelines.
2. Run 00_setup_and_reset.py.
3. Run 01_seed_csv.py.
4. Full refresh Pipeline 1.
5. Full refresh Pipeline 2.
6. Restart at 04_assert_initial_state.py.
```

A full refresh of only one part of the graph is not a substitute for this
clean POC reset.

## Common failure interpretation

### CDF error for an early table version

Cause:

```text
an older script queried table_changes(..., 0)
```

Action:

```text
use the supplied POC 2.1 scripts, which query only the exact mutation version
```

### More than two C002 Satellite versions

Cause:

```text
state from multiple test cycles or an uncoordinated upstream rebuild
```

Action:

```text
perform the complete clean reset; do not weaken the assertions
```

### C002 remains active after the delete

Check:

```text
Was Pipeline 2 updated after 07?
Was Pipeline 1 accidentally rerun and C002 recreated?
Are both pipelines publishing to the same catalog and schema?
```

### DQX imports fail

Cause:

```text
Pipeline 1 does not have the databricks-labs-dqx dependency
```

### Delete marker reason is questionable

The POC may use `SOURCE_DELETE` only because the controlled manual DQX delete
is the sole delete producer. Do not enable DQX-valid Auto-TTL and continue
using that classification. Raw CDF does not contain deletion origin.

## Success decision

POC 2.1 is experimentally validated only when:

```text
all assertion notebooks pass
the no-input convergence run passes
the observed versions and environment are recorded
the evidence package is retained
```

Only then should the shared-library implementation prompt be used.
