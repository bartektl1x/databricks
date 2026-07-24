# POC 2.1 — Data Vault deletion provenance

## Objective

Demonstrate a small, managed-streaming deletion design that another agent can
later translate into an existing metadata-driven Lakeflow Declarative
Pipelines shared library.

The POC proves:

```text
source update
-> old Satellite version closes
-> new active Satellite version is created
-> closure is not classified as deletion

source delete
-> active Satellite version closes
-> no replacement Satellite version is created
-> Hub remains unchanged
-> one durable deletion marker is stored
-> only the delete-closed version is classified as deletion
-> Gold excludes the customer
```

Read [ARCHITECTURE.md](ARCHITECTURE.md) before using this as a framework
reference.

## Relationship to validated POC 2

POC 2.1 extends rather than replaces POC 2.

Preserved:

- two Lakeflow pipelines;
- CSV Volume and Auto Loader Bronze;
- keyless Bronze;
- DQX valid/quarantine routing;
- `_ingested_at` preservation;
- deferred Auto-TTL;
- `skipChangeCommits` on Bronze-to-DQX;
- published DQX-valid CDF;
- AUTO CDC SCD Type 2;
- Gold current-state materialized view;
- exact-version CDF assertions.

Added:

- deterministic customer Hub hash key;
- insert-only Data Vault Hub;
- explicit Satellite history columns;
- deterministic C002 update phase;
- durable SCD Type 1 deletion markers;
- classified Satellite materialized view;
- update-versus-delete assertions;
- no-new-input replay check.

The validated `poc1/` and `poc2/` files are not modified.

## Namespace

Every file intentionally uses:

```python
CATALOG = "main"
SCHEMA = "demo"
```

Replace them consistently for your workspace. Configure both pipelines to
publish to the same catalog and schema.

All POC 2.1 object names use the `poc21_` prefix, so the package can coexist
with POC 1 and POC 2.

## Pipeline 1

Source:

```text
02_retention_pipeline.py
```

Graph:

```text
CSV Volume
-> Auto Loader Bronze
-> DQX annotated temporary view
-> DQX-valid staging streaming table with CDF
-> DQX-quarantine streaming table
```

Required pipeline dependency:

```text
databricks-labs-dqx
```

Auto-TTL is not active. `_ingested_at` and `skipChangeCommits` preserve the
future retention contract.

## Pipeline 2

Source:

```text
03_cdc_marker_and_gold_pipeline.py
```

Graph:

```text
published DQX-valid CDF
-> deterministic customer_hk
-> insert-only AUTO CDC SCD1 Hub
-> AUTO CDC SCD2 Satellite
-> AUTO CDC SCD1 deletion markers
-> classified Satellite materialized view
-> Gold current-state materialized view
```

Use a current supported Lakeflow environment with AUTO CDC and cross-pipeline
CDF support.

## Databricks prerequisites

- Both pipelines must publish their datasets to Unity Catalog.
- Both pipelines must publish to the same catalog and schema configured in
  the source files.
- Use triggered pipeline mode for the ordered POC procedure.
- Pipeline 2 must run on a current environment that supports reading CDF from
  a published pipeline streaming table.
- Run the `UPDATE` and `DELETE` scripts on compute that supports DML against
  Unity Catalog streaming tables, such as a compatible shared Unity Catalog
  cluster or SQL warehouse.
- Pipeline 1 requires the `databricks-labs-dqx` dependency.

The POC uses DML only to produce deterministic CDF events at the already
validated DQX boundary. Production deletion-event creation is a separate
design decision.

## Files

```text
00_setup_and_reset.py
01_seed_csv.py
02_retention_pipeline.py
03_cdc_marker_and_gold_pipeline.py
04_assert_initial_state.py
05_update_dqx_valid_row.py
06_assert_update_propagation.py
07_delete_dqx_valid_row.py
08_assert_delete_propagation.py
09_show_evidence.py
validate_source_package.py
ARCHITECTURE.md
COMPLETION-AUDIT.md
DATABRICKS-RUNBOOK.md
README.md
VALIDATION.md
```

For deployment, follow [DATABRICKS-RUNBOOK.md](DATABRICKS-RUNBOOK.md).

## Local source validation

Before copying the files to Databricks, run:

```text
python validate_source_package.py
```

This checks package completeness, Python syntax, and the presence of critical
Hub/Satellite/marker contracts. It does not validate Lakeflow runtime
behavior.

Record workspace results in [VALIDATION.md](VALIDATION.md).

## Exact execution order

### Initial load

1. Stop both POC 2.1 pipelines.
2. Run `00_setup_and_reset.py`.
3. Run `01_seed_csv.py`.
4. FULL REFRESH Pipeline 1.
5. FULL REFRESH Pipeline 2.
6. Run `04_assert_initial_state.py`.

### Update phase

7. Run `05_update_dqx_valid_row.py`.
8. Run a NORMAL INCREMENTAL update of Pipeline 2 only.
9. Run `06_assert_update_propagation.py`.

### Delete phase

10. Run `07_delete_dqx_valid_row.py`.
11. Run a NORMAL INCREMENTAL update of Pipeline 2 only.
12. Run `08_assert_delete_propagation.py`.

### Replay/idempotency check

13. Run Pipeline 2 normally again with no new input.
14. Rerun `08_assert_delete_propagation.py`.
15. The assertion must still report exactly one marker.

### Evidence

16. Run `09_show_evidence.py`.

Do not run Pipeline 1 after the initial full refresh. It would re-evaluate
Bronze and can invalidate the controlled DQX mutation sequence.

## Expected initial state

```text
Bronze rows:             5
DQX-valid rows:          3
DQX-quarantine rows:     2
Hub rows:                3
Satellite active rows:   3
Satellite closed rows:   0
Deletion markers:        0
Gold rows:               3
```

Valid customers:

```text
C001
C002
C004
```

Quarantine:

```text
C003               invalid email
Missing Identifier missing customer_id
```

## Expected state after C002 update

The scripts dynamically capture `update_version`.

```text
Hub C002 rows:             1
Satellite C002 versions:   2
Satellite active versions: 1
Deletion markers:          0
Delete-classified rows:    0
Gold C002 rows:            1
```

First version:

```text
email           = bob@example.com
city            = Los Angeles
__END_AT         = update_version
closed_by_delete = false
```

Second version:

```text
email           = bob.updated@example.com
city            = Seattle
__START_AT       = update_version
__END_AT         = null
closed_by_delete = false
```

## Expected state after C002 delete

The scripts dynamically capture `delete_version`.

```text
DQX-valid C002 rows:        0
Hub C002 rows:              1
Satellite C002 versions:    2
Satellite active versions:  0
Deletion markers:           1
Delete-classified rows:     1
Gold C002 rows:             0
```

First version:

```text
__END_AT             = update_version
closed_by_delete     = false
deletion_reason      = null
delete_sequence      = null
```

Second version:

```text
__END_AT             = delete_version
closed_by_delete     = true
deletion_reason      = SOURCE_DELETE
delete_sequence      = delete_version
deletion_request_id  = null
```

## Why the marker is separate

AUTO CDC `apply_as_deletes` closes the active SCD2 version. It does not create
a replacement row on which an `is_deleted` flag can be stored.

The POC preserves:

```text
deleted customer has no active Satellite version
```

The companion marker stores delete provenance without changing Satellite
business semantics.

## Why exact CDF versions are used

Pipeline-managed tables can have Delta versions that predate CDF recording.
CDF is not retroactive.

The scripts therefore:

```text
execute one mutation
-> read DESCRIBE HISTORY
-> capture its exact operation version
-> query table_changes(table, version, version)
```

They never query `table_changes(..., 0)`.

## Replay limitations

The no-new-input incremental rerun proves ordinary checkpoint convergence.

It does not prove arbitrary full-refresh recovery. If historical source
deletes have fallen outside retained CDF, a full refresh cannot reconstruct
the marker or historical closed Satellite versions from current DQX state.

Treat full refresh as a coordinated recovery operation.

## Explicit non-goals

This POC does not implement:

- production shared-library classes;
- metadata/configuration tables;
- active Auto-TTL;
- GDPR request orchestration;
- physical historical deletion;
- `REORG TABLE ... APPLY (PURGE)`;
- `VACUUM` orchestration;
- raw-file erasure;
- replay suppression;
- legal holds;
- Link deletion semantics.

## Future extension points

### Metadata-driven configuration

Reuse existing framework metadata for:

```text
Hub hash key
Satellite parent key
AUTO CDC sequence
Satellite payload/history columns
object naming
```

Add deletion-marker configuration only as an opt-in extension. Do not create
an independent competing Data Vault key model.

### Independent Bronze/DQX retention

After predictive optimization is available:

```text
Bronze Auto-TTL       local raw-layer lifecycle
DQX valid Auto-TTL    local valid-layer lifecycle
DQX quarantine TTL   local error-layer lifecycle
```

Bronze-to-DQX keeps `skipChangeCommits` so Bronze maintenance commits do not
break the stream.

DQX-valid requires an additional safeguard: it is also the business CDF source
for Pipeline 2. Raw CDF cannot distinguish Auto-TTL DELETE from a controlled
business DELETE. Do not simply enable the commented DQX-valid Auto-TTL policy;
first introduce an explicit deletion-origin ledger or separate business CDC
source. Otherwise retention expiration would be incorrectly classified as
`SOURCE_DELETE`.

### Additional deletion reasons

A future enriched deletion-event contract can provide:

```text
SOURCE_DELETE
RETENTION_EXPIRED
GDPR_ERASURE
ADMINISTRATIVE_DELETE
DATA_CORRECTION
```

Raw CDF alone cannot infer these reasons.

### Auditability

The marker is durable deletion provenance, not a complete execution audit.

A later control plane can add:

```text
deletion_request_id
source object and source generation
request/effective timestamps
processing status
attempt and error history
verification evidence
```

Avoid storing unnecessary erased PII in a long-lived audit table.

### Replay and resurrection prevention

Normal checkpoint replay is covered by the marker's logical event key. Future
GDPR handling needs a stronger erased-subject registry or authoritative
source cleanup so old files, full refreshes, and disaster recovery cannot
restore erased subjects.

AUTO CDC also retains delete tombstones temporarily to reject late,
out-of-order events. Its default retention is not a permanent resurrection
barrier. A production integration must set
`pipelines.cdc.tombstoneGCThresholdInSeconds` longer than the maximum expected
delay between source-event creation and pipeline processing, then combine
that with an authoritative deletion or erased-subject ledger for longer-term
replay protection.

### GDPR physical erasure

GDPR requires a separate governed workflow covering historical PII,
upstream/raw data, physical cleanup, audit, and resurrection prevention.

Logical Satellite closure plus a marker is a prerequisite, not completion.

## Official references

- [AUTO CDC overview](https://docs.databricks.com/aws/en/ldp/cdc)
- [`create_auto_cdc_flow` Python reference](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes)
- [Advanced AUTO CDC and target CDF](https://docs.databricks.com/aws/en/ldp/cdc-advanced)
- [`table_changes` reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/table_changes)
- [Pipeline refresh semantics](https://docs.databricks.com/aws/en/ldp/concepts/refresh)
- [Auto-TTL](https://docs.databricks.com/aws/en/tables/operations/auto-ttl)
- [GDPR preparation](https://docs.databricks.com/aws/en/ldp/gdpr)
