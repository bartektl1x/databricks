# POC 2.1 architecture and invariants

## Purpose

This POC is a teaching reference for adding deletion propagation to a future
metadata-driven Lakeflow Declarative Pipelines shared library.

It extends the validated POC 2 architecture. It does not redesign Bronze or
DQX, and it does not attempt to implement the eventual production metadata
platform.

## Data flow

```text
Pipeline 1

CSV in Unity Catalog Volume
    |
    v
Auto Loader Bronze streaming table
    |
    v
DQX annotated temporary view
    |--------------------------|
    v                          v
DQX-valid staging          DQX quarantine
streaming table            streaming table
with Delta CDF


Pipeline 2

Published DQX-valid CDF
    |
    +--> deterministic customer_hk
    |       |
    |       +--> INSERT events only
    |       |       -> AUTO CDC SCD1 Customer Hub
    |       |
    |       +--> all non-preimage events
    |       |       -> AUTO CDC SCD2 Customer Satellite
    |       |
    |       `--> DELETE events only
    |               -> AUTO CDC SCD1 durable deletion markers
    |
    +--> Satellite LEFT JOIN markers
    |       -> classified Satellite materialized view
    |
    `--> Hub INNER JOIN active Satellite
            -> Gold current-state materialized view
```

## Deterministic hash key

Every keyed flow uses:

```text
customer_hk =
    SHA-256("CUSTOMER" || "||" || UPPER(TRIM(customer_id)))
```

The POC uses a hex string because it is easy to inspect. A production
framework must reuse its existing canonicalization, delimiter, encoding, and
hash algorithm rather than introducing this POC algorithm independently.

The POC treats `customer_id` as an immutable business key. A production
business-key correction must follow the shared library's explicit Data Vault
policy, such as creating a new identity or processing a governed
delete-and-insert sequence. It must not silently mutate a Hub key.

## Deterministic sequence

The Satellite and marker use the DQX-valid Delta `_commit_version`.

This is correct inside this POC because:

- both outputs consume the same DQX-valid table history;
- Delta commit versions totally order changes in that table;
- update and delete assertions query the exact source commit;
- `Satellite.__END_AT` and `marker.delete_sequence` share the same sequence
  domain.

`_commit_version` is not a global business sequence and must not be compared
across unrelated Delta tables.

## CDF mode

The POC keeps the validated POC 2 contract:

```text
legacy Delta CDF
delta.enableChangeDataFeed = true
```

Current Databricks documentation also describes automatic CDF as a Public
Preview capability on newer runtimes. It uses the same `readChangeFeed` and
`table_changes` APIs, but it has separate runtime and table-feature
requirements. A future framework may evaluate that mode independently.
Changing CDF modes is not required to implement the deletion-provenance
design and must not be mixed into the first shared-library integration.

## Hub invariant

The Hub represents durable business-key identity:

```text
customer_hk
customer_id
record_source
hub_loaded_at
```

Its source accepts CDF `insert` events only.

Therefore:

- initial customer discovery creates one Hub row;
- descriptive updates do not version or replace the Hub;
- source deletes do not remove the Hub;
- deletion affects the Satellite's active state, not Hub identity.

Link deletion semantics are deliberately out of scope.

## Satellite invariant

The Satellite is keyed by `customer_hk` and uses SCD Type 2.

Only these descriptive columns create history:

```text
name
email
city
updated_at
```

Operational metadata and DQX diagnostic changes do not create business
history by default.

An update:

```text
closes old version at update sequence
creates new active version at update sequence
```

A delete:

```text
closes active version at delete sequence
creates no replacement version
```

## Deletion-provenance invariant

`__END_AT IS NOT NULL` is not a deletion predicate. Both updates and deletes
close SCD2 versions.

The durable marker target records only actual DQX CDF delete events:

```text
customer_hk
customer_id
delete_sequence
deleted_at
deletion_reason
deletion_request_id
```

Logical marker identity:

```text
customer_hk + delete_sequence
```

Classification is:

```text
Satellite.customer_hk = marker.customer_hk
AND Satellite.__END_AT = marker.delete_sequence
```

The POC reason `SOURCE_DELETE` means only that Pipeline 2 observed a source
CDF delete under the POC invariant that the only DQX-valid delete producer is
the controlled business-delete test. It is not a legal classification.

## Gold invariant

Gold is the inner join of:

```text
Hub
active Satellite (__END_AT IS NULL)
```

The Hub remains after C002 is deleted, but C002 has no active Satellite
version, so the inner join excludes it.

## Replay and refresh boundary

### Normal restart

AUTO CDC checkpoints and marker keys make normal incremental restart
convergent. A no-new-input Pipeline 2 update must not add a marker.

AUTO CDC temporarily retains internal delete tombstones so older,
out-of-order events do not immediately recreate deleted state. Databricks
documents a default tombstone retention interval of two days, configurable
with:

```text
pipelines.cdc.tombstoneGCThresholdInSeconds
```

That interval must exceed the maximum expected event-arrival and processing
delay in a production integration. It is distinct from the durable marker:

```text
AUTO CDC tombstone
    short-lived ordering and late-event protection

deletion marker
    durable provenance and closure classification
```

### Full refresh

A full refresh clears streaming-table data and flow checkpoints and
reprocesses available input.

If a historical delete is no longer present in retained CDF:

- the marker cannot be reconstructed from current DQX state;
- historical closed Satellite versions cannot be reconstructed from the
  current snapshot alone.

For this iteration:

```text
DQX, Hub, Satellite, and marker full refresh
= coordinated exceptional recovery operation
```

A later production design needs an authoritative replay source or deletion
ledger if historical reconstruction must survive source-CDF expiration.
It also needs an erased-subject suppression mechanism if old raw inputs can
be replayed after AUTO CDC tombstones have expired.

## Semantic separation

### Source delete

```text
business deletion event
-> DQX CDF
-> Satellite logical closure
-> durable marker
-> Gold removal
```

### Retention expiration

```text
local table lifecycle cleanup
-> future independent Auto-TTL
-> downstream append stream protected by skipChangeCommits
-> not automatically treated as a business deletion reason
```

Bronze and DQX may eventually use different retention windows, but the
boundaries are not equivalent:

- Bronze Auto-TTL commits can be ignored by Bronze-to-DQX through
  `skipChangeCommits`. DQX enforces its own lifecycle.
- DQX quarantine has no business-CDC consumer in this POC and can expire
  independently.
- DQX valid is the business CDF source for Pipeline 2. Raw CDF does not carry
  deletion origin. If DQX-valid Auto-TTL were enabled without another design
  change, this POC would consume its retention deletes as `SOURCE_DELETE`.

Therefore DQX-valid Auto-TTL is not a plug-in change to this reference. Before
enabling it, a future design must introduce one of these explicit contracts:

```text
governed deletion event/ledger enriched with reason
separate business CDC source distinct from retention cleanup
another verified mechanism that identifies and suppresses retention deletes
```

Do not infer delete reason from Delta operation timing, commit text, or
`__END_AT`.

### GDPR erasure

```text
future governed request
-> logical propagation
-> physical removal or irreversible anonymization
-> upstream/raw cleanup
-> purge/vacuum maintenance
-> replay suppression
-> auditable completion
```

Closing a Satellite row and writing a marker do not complete GDPR erasure.

## Future metadata mapping

A later shared-library integration can derive:

| POC concept | Future metadata source |
|---|---|
| DQX CDF source | published staging dataset metadata |
| `customer_hk` | existing Hub/Data Vault key configuration |
| Satellite key | existing parent Hub hash key |
| sequence | existing AUTO CDC sequence configuration |
| history columns | Satellite payload/hash-diff columns |
| marker target name | naming conventions plus opt-in deletion config |
| default reason | deletion-propagation configuration |
| Gold active join | existing Gold/Data Vault relationship metadata |

Do not add a second, incompatible key or hash configuration solely for
deletion handling.
