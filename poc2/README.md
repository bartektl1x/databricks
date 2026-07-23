# Databricks streaming retention/deletion — POC 2

## Scope

This POC validates:

```text
CSV Unity Catalog Volume
  -> Auto Loader Bronze streaming table + Auto-TTL
  -> DQX valid/quarantine streaming tables + independent Auto-TTL
  -> DQX-valid legacy CDF
  -> AUTO CDC SCD Type 2 satellite
  -> Gold current-state materialized view
```

It is hard-coded and isolated from production framework classes.

It does not redesign the validated POC 1 AUTO CDC pattern.

## Namespace

```text
Catalog: dev_mr_dhc_bronze
Schema:  slpat_landing_staging
```

## Required pipeline configuration

Create two Lakeflow pipelines.

### Pipeline 1 — retention

Source:

```text
02_retention_pipeline.py
```

Publication target:

```text
Catalog: dev_mr_dhc_bronze
Schema:  slpat_landing_staging
```

Add the following environment dependency:

```text
databricks-labs-dqx
```

Use a runtime/channel supporting Auto-TTL. Auto-TTL policy creation requires
Databricks Runtime 17.3 or later and predictive optimization.

### Pipeline 2 — CDC and Gold

Source:

```text
03_cdc_and_gold_pipeline.py
```

Publication target:

```text
Catalog: dev_mr_dhc_bronze
Schema:  slpat_landing_staging
```

Use a runtime supporting published streaming-table CDF and AUTO CDC.

## Execution order

1. Run `00_setup_and_reset.py`.
2. Run `01_seed_csv.py`.
3. Run a **FULL REFRESH** of Pipeline 1.
4. Run a **FULL REFRESH** of Pipeline 2.
5. Run `04_assert_initial_state.py`.
6. Run `05_delete_dqx_valid_row.py`.
7. Run a **NORMAL INCREMENTAL** update of Pipeline 2.
8. Run `06_assert_delete_propagation.py`.
9. Run `07_show_evidence.py`.

## Why the deterministic delete is explicit

Auto-TTL runs asynchronously. Databricks does not guarantee the exact time at
which predictive optimization performs the DELETE.

The POC therefore proves two facts independently:

1. Auto-TTL policies are configured on Bronze, DQX valid, and DQX quarantine.
2. A DELETE on DQX valid produces CDF that AUTO CDC consumes to close SCD2
   history and remove the customer from Gold.

`05_delete_dqx_valid_row.py` produces the deterministic DQX-valid DELETE.
Operational Auto-TTL execution can later be observed through:

```text
system.storage.predictive_optimization_operations_history
DESCRIBE HISTORY <table>
```

## Important semantic boundary

Bronze -> DQX uses:

```python
.option("skipChangeCommits", "true")
```

This prevents Bronze Auto-TTL change commits from failing the DQX stream.
It does not propagate Bronze deletes.

DQX valid owns its own Auto-TTL policy. Its CDF delete events are consumed by
Pipeline 2 and applied to the SCD2 satellite.

SCD2 deletion closes the active version. It does not physically erase the
historical satellite row.
