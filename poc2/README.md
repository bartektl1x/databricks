# POC 2 — DQX deletion propagation without active Auto-TTL

## Main objective

```text
CSV Volume -> Auto Loader Bronze -> DQX valid/quarantine
DQX-valid DELETE -> Delta CDF -> AUTO CDC SCD2 Silver -> Gold removal
```

Every file intentionally uses:

```python
CATALOG = "main"
SCHEMA = "demo"
```

Replace those values manually in every file. Configure both Lakeflow pipelines to publish to the same catalog and schema.

## Why TTL is deferred

Predictive optimization is unavailable. This POC does not enable it, does not configure active `auto_ttl`, and does not assert TTL properties. It preserves `_ingested_at`, future independent policy placeholders, and `skipChangeCommits` on Bronze -> DQX.

## Pipelines

Pipeline 1 source: `02_retention_pipeline.py`

Pipeline 1 dependency:

```text
databricks-labs-dqx
```

Pipeline 2 source: `03_cdc_and_gold_pipeline.py`

## Execution order

1. `00_setup_and_reset.py`
2. `01_seed_csv.py`
3. FULL REFRESH Pipeline 1
4. FULL REFRESH Pipeline 2
5. `04_assert_initial_state.py`
6. `05_delete_dqx_valid_row.py`
7. NORMAL INCREMENTAL update Pipeline 2
8. `06_assert_delete_propagation.py`
9. `07_show_evidence.py`

Do not rerun Pipeline 1 between steps 6 and 8.

## Expected final C002 state

```text
DQX-valid rows:       0
CDF delete events:    1
Silver history rows:  1
Silver active rows:   0
Silver closed rows:   1
Gold rows:            0
```

This proves logical SCD2 deletion, not physical historical erasure.
