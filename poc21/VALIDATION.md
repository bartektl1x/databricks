# POC 2.1 validation status

## Evidence boundary

POC 1 and POC 2 were successfully executed in the user's Databricks
workspace.

POC 2.1 extends that validated baseline, but its new Hub, update,
deletion-marker, and classification flows have not yet been executed in
Databricks.

Do not describe POC 2.1 as runtime validated until every workspace gate below
passes.

## Local verification

Executed:

```text
python validate_source_package.py
```

Result:

```text
POC 2.1 source package validation passed.
Validated 15 required files.
All Python files parsed successfully.
Critical Hub, Satellite, marker, classification, and Gold contracts found.
Databricks runtime validation is still required.
```

This proves source completeness and syntax only.

## Requirement evidence matrix

| Requirement | Code evidence | Local status | Databricks status |
|---|---|---|---|
| Auto Loader Bronze | `02_retention_pipeline.py` | Present | Pending |
| DQX valid/quarantine | `02_retention_pipeline.py` | Present | Pending |
| Published staging CDF | DQX valid table property and Pipeline 2 CDF view | Present | Pending |
| Deterministic `customer_hk` | `customer_hash_key()` plus independent assertions | Present | Pending |
| Persistent Hub | Insert-only Hub view and SCD1 flow | Present | Pending |
| SCD2 Satellite | Hash-keyed AUTO CDC flow | Present | Pending |
| Explicit history payload | `track_history_column_list` | Present | Pending |
| Update closes old version | `05` and `06` | Asserted | Pending |
| Update creates active version | `06` | Asserted | Pending |
| Update creates no marker | `06` | Asserted | Pending |
| Delete closes active version | `07` and `08` | Asserted | Pending |
| Delete creates no replacement | `08` active-count assertion | Asserted | Pending |
| Hub remains after delete | `08` Hub assertions | Asserted | Pending |
| Durable marker | SCD1 marker flow and `08` | Present/asserted | Pending |
| Exact closure classification | Classified MV and `08` | Present/asserted | Pending |
| Gold excludes deleted customer | Hub/active-Satellite MV and `08` | Asserted | Pending |
| No fixed CDF start at zero | exact operation-version queries | Present | Pending |
| Normal no-input rerun converges | README run step and repeatable `08` | Specified | Pending |
| Full-refresh limitation | `ARCHITECTURE.md` | Documented | Not claimed |
| Tombstone versus durable-marker boundary | `ARCHITECTURE.md` | Documented | Not claimed |
| Requirement-by-requirement completion audit | `COMPLETION-AUDIT.md` | Present | Runtime gates pending |

## Databricks runtime gates

Record the actual output for every gate.

### Gate 1: initial load

- [ ] Reset succeeds.
- [ ] CSV seed succeeds.
- [ ] Pipeline 1 full refresh succeeds.
- [ ] Pipeline 2 full refresh succeeds.
- [ ] `04_assert_initial_state.py` passes.
- [ ] C002 Hub hash matches the documented normalization.

### Gate 2: update

- [ ] `05_update_dqx_valid_row.py` succeeds.
- [ ] Pipeline 2 normal update succeeds.
- [ ] `06_assert_update_propagation.py` passes.
- [ ] Exactly two C002 Satellite versions exist.
- [ ] Exactly zero C002 markers exist.
- [ ] Hub load metadata remains from initial discovery.

Observed update version:

```text
PENDING
```

### Gate 3: delete

- [ ] `07_delete_dqx_valid_row.py` succeeds.
- [ ] Pipeline 2 normal update succeeds.
- [ ] `08_assert_delete_propagation.py` passes.
- [ ] Exactly one C002 Hub row remains.
- [ ] No active C002 Satellite version exists.
- [ ] Exactly one marker exists.
- [ ] Exactly one version has `closed_by_delete = true`.
- [ ] Gold excludes C002.

Observed delete version:

```text
PENDING
```

### Gate 4: no-new-input rerun

- [ ] Run Pipeline 2 normally without new input.
- [ ] Rerun `08_assert_delete_propagation.py`.
- [ ] Marker count remains exactly one.
- [ ] Satellite history remains exactly two C002 versions.

### Gate 5: evidence

- [ ] Run `09_show_evidence.py`.
- [ ] Save pipeline event logs or screenshots.
- [ ] Replace the `PENDING` versions above.
- [ ] Record environment/runtime/channel and pipeline publication settings.

## Completion rule

The reference POC is implementation-complete locally.

It becomes experimentally validated only after Gates 1 through 5 pass in
Databricks. Until then, downstream framework integration should treat the new
design as a reviewed candidate, not as proven runtime behavior.
