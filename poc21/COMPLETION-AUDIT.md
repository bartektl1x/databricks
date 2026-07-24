# POC 2.1 completion audit

## Audit rule

This document maps every requested capability to authoritative evidence.
Source presence proves only implementation intent. A behavioral requirement
is not experimentally proven until its Databricks runtime gate passes.

## Architecture and scope

| Requirement | Authoritative evidence | Status |
|---|---|---|
| Extend validated POC 2 rather than redesign it | `02_retention_pipeline.py`, `README.md` relationship section | Implemented; runtime pending |
| CSV Volume to Auto Loader Bronze | `01_seed_csv.py`, Bronze definition in Pipeline 1 | Implemented; runtime pending |
| DQX valid and quarantine staging | Pipeline 1 plus initial assertions | Implemented; runtime pending |
| Data Vault Hub | Insert-only Hub source and SCD1 Hub flow in Pipeline 2 | Implemented; runtime pending |
| SCD2 Satellite | Hash-keyed AUTO CDC flow in Pipeline 2 | Implemented; runtime pending |
| Gold current-state materialized view | Hub joined to active Satellite in Pipeline 2 | Implemented; runtime pending |
| Small and isolated | `poc21_` namespace, hard-coded `main.demo`, no framework classes | Proven by source inspection |
| Copy-paste-ready ordered execution | `README.md` | Present |

## Identity, sequencing, and managed streaming

| Requirement | Authoritative evidence | Status |
|---|---|---|
| Deterministic hash key | `customer_hash_key()` and independent `hashlib` assertions | Implemented; runtime pending |
| Hub, Satellite, marker, and Gold share the hash key | Pipeline 2 joins and AUTO CDC keys | Implemented |
| Deterministic source-local sequence | DQX `_commit_version` in Satellite and marker flows | Implemented |
| Exact update/delete evidence | `05`, `07`, and exact inclusive `table_changes(start, end)` queries | Implemented; runtime pending |
| No CDF version-zero assumption | All CDF tests resolve the exact mutation version | Proven by source audit |
| Managed streaming semantics | Published DQX CDF plus Lakeflow AUTO CDC targets | Implemented; runtime pending |
| Only business payload creates Satellite history | `track_history_column_list` | Implemented; runtime pending |

## Required update behavior

| Requirement | Authoritative evidence | Status |
|---|---|---|
| Old Satellite version closes | `06_assert_update_propagation.py` exact `__END_AT` assertion | Runtime pending |
| New active version is created | `06` exact `__START_AT` and active-count assertions | Runtime pending |
| Hub is unchanged | `06` Hub count and load-time assertions | Runtime pending |
| Update creates no marker | `06` marker-count assertion | Runtime pending |
| Update closure is not classified as delete | `06` classified-view assertions | Runtime pending |
| Gold exposes updated state | `06` Gold payload assertions | Runtime pending |

## Required delete behavior

| Requirement | Authoritative evidence | Status |
|---|---|---|
| Active Satellite version closes | `08_assert_delete_propagation.py` exact delete-sequence assertion | Runtime pending |
| No replacement version is created | `08` zero-active and total-history assertions | Runtime pending |
| Hub remains unchanged | `08` Hub count and identity assertions | Runtime pending |
| Gold excludes customer | `08` Gold count and active-ID assertions | Runtime pending |
| Durable marker identifies the event | Marker SCD1 flow plus `08` marker assertions | Runtime pending |
| Only the delete-closed version is classified | Classified MV join plus `08` positive/negative assertions | Runtime pending |
| Classification does not infer from non-null `__END_AT` | Join on `(customer_hk, __END_AT = delete_sequence)` | Implemented |

## Replay and recovery

| Requirement | Authoritative evidence | Status |
|---|---|---|
| Duplicate checkpoint consumption converges | Marker key `(customer_hk, delete_sequence)` and no-input rerun gate | Runtime pending |
| Full refresh is not overclaimed | `ARCHITECTURE.md` and `README.md` recovery boundary | Documented |
| CDF is not treated as permanent audit storage | Durable marker plus recovery documentation | Implemented/documented |
| AUTO CDC tombstone is distinguished from marker | `ARCHITECTURE.md` | Documented |
| Long-term resurrection prevention remains future scope | `ARCHITECTURE.md`, `README.md` | Documented |

## Required future extension points

| Extension | Evidence | Status |
|---|---|---|
| Metadata-driven configuration | README and architecture metadata mapping | Documented |
| Independent Bronze Auto-TTL | Pipeline 1 placeholders and README | Documented, inactive |
| `skipChangeCommits` behavior | Pipeline 1 and semantic-boundary documentation | Implemented as hook |
| Additional deletion reasons | README enumeration and provenance ADR | Documented |
| GDPR physical erasure | README and architecture | Explicitly deferred |
| Auditability | Durable marker and future audit fields | Documented |
| Replay/resurrection prevention | Tombstone and authoritative-ledger discussion | Documented |

## Semantic separation

The implementation and documentation keep these contracts separate:

```text
source delete
    controlled business event
    -> CDF -> Satellite closure -> durable SOURCE_DELETE marker

retention expiration
    local table lifecycle cleanup
    -> future independent Auto-TTL
    -> not inferred to be a business delete

GDPR erasure
    future governed physical removal or irreversible anonymization
    -> not claimed by logical SCD2 closure
```

The current POC deliberately does not enable DQX-valid Auto-TTL, because raw
CDF does not identify whether a Delta `DELETE` originated from retention or a
business deletion.

## Non-goal audit

The package contains no implementation of:

- production shared-library classes;
- metadata platform abstractions;
- active Auto-TTL;
- GDPR workflow orchestration;
- historical physical purge;
- Link deletion semantics;
- production framework integration.

## Completion decision

The reference package is source-complete and locally auditable.

The following evidence is still missing:

```text
Databricks Gates 1-5 in VALIDATION.md
```

Therefore the design is ready for workspace execution, but runtime completion
must not be claimed until those gates are recorded.
