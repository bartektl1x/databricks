Enterprise Data Lifecycle Management (DLM) for Lakeflow SDP
Target architecture
Raw Files (ADLS)

        │
        ▼
Bronze (Streaming Table - Auto Loader)

        │
        ▼
DQ (Streaming Table)

        │
        ▼
Silver (Streaming Table - RDV + SCD2 + CDC)

        │
        ▼
Gold (Materialized Views)

This design assumes:

Lakeflow SDP (Streaming Tables)
Unity Catalog
Delta Lake
CDF enabled where appropriate
Large enterprise (hundreds/thousands of tables)
GDPR readiness
Minimal operational burden
Fully metadata-driven
Design philosophy

Deletion is not a pipeline responsibility.

Deletion is a platform capability called Data Lifecycle Management (DLM).

Pipelines only move data.

DLM decides

when data expires
what should be deleted
how deletion propagates
when physical cleanup occurs
auditing

This separation makes pipelines almost completely independent from governance.

Architecture
                    Data Lifecycle Management

               ┌─────────────────────────────┐
               │ Lifecycle Metadata          │
               │ Retention Policies          │
               │ GDPR Requests               │
               │ Entity Mapping              │
               │ Watermarks                  │
               └─────────────┬───────────────┘
                             │
                  Lifecycle Orchestrator
                             │
      ┌─────────────┬─────────┴─────────────┬────────────┐
      │             │                       │            │
Retention Job   GDPR Job            Cleanup Job    Raw Cleanup
      │             │                       │            │
      └─────────────┴──────────────┬────────┘
                                   │
                     Bronze → DQ → Silver → Gold
Metadata Layer

I would split metadata into independent tables.

1. lifecycle_table_config

Master configuration.

Column	Type	Description
catalog	STRING	UC catalog
schema	STRING	UC schema
table_name	STRING	target table
layer	STRING	bronze/silver/gold
enabled	BOOLEAN	enable lifecycle
contains_pii	BOOLEAN	GDPR applicable
retention_policy	STRING	FK
deletion_column	STRING	timestamp used for retention
business_key	STRING	customer_id etc
delete_strategy	STRING	HARD_DELETE / TOMBSTONE
cdf_enabled	BOOLEAN	CDF expected
cleanup_enabled	BOOLEAN	REORG/VACUUM
ingestion_watermark_enabled	BOOLEAN	replay protection
2. retention_policy

Reusable policies.

policy	retention_days	grace_days	delete_mode
bronze	180	30	HARD_DELETE
customer	3650	30	TOMBSTONE
telemetry	30	7	HARD_DELETE

Instead of configuring every table individually.

3. entity_mapping

Used only by GDPR.

entity	table	key_column
Customer	silver.customer	customer_id
Customer	gold.orders	customer_id
Employee	silver.employee	employee_id

Allows automatic discovery of affected tables.

4. deletion_requests

Append-only audit table.

request_id	entity	key	requested_at	completed_at	status	reason

Example

Customer

12345

GDPR
5. ingestion_watermark

Replay protection.

table	minimum_file_modification_time	updated_at

Purpose:

Prevent re-reading already expired files after

checkpoint reset
pipeline recreation
environment cloning
disaster recovery

This is NOT retention.

This is ingestion protection.

Bronze

Responsibilities

Auto Loader
Schema evolution
Basic metadata
CDF enabled

Retention is NOT implemented inside Bronze.

Bronze only respects ingestion watermark.

Example

.where(
    _metadata.file_modification_time >
    watermark
)

This protects against replay.

Retention still happens later.

DQ

Nothing special.

DQ simply propagates

Insert

Update

Delete

events downstream.

No lifecycle logic.

Silver

Silver becomes the lifecycle boundary.

Reasons

business keys exist
SCD2 already implemented
CDC available
entities clearly identified

Deletion propagates through CDC.

Delete strategy

Two supported strategies.

Hard Delete

Good for

telemetry
logs
temporary data
DELETE
Tombstone

Recommended for

customer
finance
business master data

Columns

is_deleted

deleted_at

deletion_reason

deletion_request_id

Benefits

audit
recovery
CDC propagation
GDPR traceability

After grace period

Physical delete occurs.

Gold

Gold never owns lifecycle logic.

Materialized Views simply refresh.

Delete

↓

Silver updated

↓

MV refresh

↓

Deleted automatically

No custom logic required.

CDF

CDF becomes the transport mechanism.

Every deletion becomes

_change_type

insert

update_postimage

delete

Silver consumes

DELETE

as normal CDC.

No custom delete pipeline required.

skipChangeCommits

This option is not the deletion mechanism.

Purpose

Prevent streaming readers from failing when upstream tables receive maintenance commits (DELETE, UPDATE, MERGE).

Without

DELETE Bronze

↓

Streaming reader

↓

Exception

With

DELETE Bronze

↓

skipChangeCommits

↓

Reader continues

Actual deletion propagation is handled through CDF.

Think of it as

Availability feature

not

Lifecycle feature

Retention Flow

Daily

Retention Policy

↓

Find expired rows

↓

DELETE (or Tombstone)

↓

CDF generated

↓

Silver updated

↓

Gold refreshed

↓

Audit written
GDPR Flow

Triggered

New Request

↓

Entity Mapping

↓

Locate tables

↓

Delete/Tombstone

↓

CDF

↓

Silver

↓

Gold

↓

Audit Complete

Same mechanism.

Different trigger.

Physical Cleanup

Never immediately.

Logical Delete

↓

Grace Period

↓

REORG APPLY(PURGE)

↓

VACUUM

Reasons

allow rollback
deletion vectors
streaming stability
Raw File Cleanup

Independent workflow.

Raw storage has its own lifecycle.

Retention expired

↓

Delete ADLS files

↓

Update watermark

↓

Audit

Completely independent from Delta retention.

Jobs
Job 1

Retention Evaluation

Frequency

Daily

Responsibilities

evaluate retention
delete/tombstone
update audit
Job 2

GDPR Processor

Frequency

Event driven

or

Every 5 minutes

Responsibilities

process requests
execute deletes
update status
Job 3

Physical Cleanup

Weekly

Responsibilities

REORG APPLY(PURGE)
Job 4

VACUUM

Weekly

Runs after cleanup.

Job 5

Raw Storage Cleanup

Weekly

Delete expired raw files.

Update ingestion watermark.

Job 6

Lifecycle Monitoring

Daily

Produces

rows deleted
rows tombstoned
pending GDPR requests
oldest request
purge backlog
raw cleanup backlog
failures

These metrics feed platform dashboards and alerts.

Failure protection
Failure	Protection
Checkpoint deleted	Ingestion watermark
Pipeline recreated	Ingestion watermark
Raw files still exist	Ingestion watermark
Delete job rerun	Idempotent deletes
GDPR rerun	Request status
REORG failure	Retry later
VACUUM failure	Retry later
Late-arriving file	Retention job removes expired rows
Future evolution

This design supports future capabilities without architectural changes:

GDPR / Right to be Forgotten
Legal Hold (prevent deletion by policy override)
Country-specific retention rules
Table-level Auto-TTL where appropriate
Soft delete vs. hard delete policies
Multiple retention classes (Bronze 180 days, Silver 7 years, Gold 3 years)
Cross-region deployments
Purview/Unity Catalog lineage integration
Central governance dashboards
Why I believe this is an L7/FAANG-grade design

The defining characteristic isn't any individual technology (CDF, tombstones, or REORG); it's the clear separation of concerns:

Pipelines are responsible only for ingestion and transformation.
Metadata defines lifecycle policies declaratively.
The DLM subsystem executes retention and GDPR workflows in an idempotent, auditable manner.
CDF is the standard mechanism for propagating deletions through Bronze → DQ → Silver.
Materialized Views in Gold naturally reflect upstream deletions after refresh.
Ingestion watermarks provide defense-in-depth against replay after checkpoint loss or environment rebuilds, without becoming the primary retention mechanism.
Physical storage cleanup (REORG, VACUUM, raw file deletion) is decoupled from logical deletion, ensuring streaming stability and operational resilience.

This separation allows the platform to scale from a handful of pipelines to thousands of tables while remaining maintainable, testable, and adaptable to new regulatory requirements with minimal impact on the data pipelines themselves. It is this architecture—not any single feature—that makes the solution suitable for a mature enterprise data platform.

Did you mention about retention policy table?

Yes, but after thinking it through more, I would actually evolve it further. I don't think the simple retention_policy table I initially proposed is sufficient for an enterprise platform. It should become a first-class policy object, similar to IAM policies or Azure Policies, rather than just storing retention_days.

I would separate what a policy is from which tables use it.

1. lifecycle_policy (master)

One row = one reusable lifecycle policy.

Column	Type	Example	Purpose
policy_name	STRING	bronze_default	Unique policy
description	STRING	Bronze retention	Documentation
retention_days	INT	180	Logical retention
grace_days	INT	30	Delay before physical purge
delete_strategy	STRING	HARD_DELETE / TOMBSTONE	Logical deletion type
cleanup_enabled	BOOLEAN	TRUE	Execute REORG/VACUUM
raw_cleanup_enabled	BOOLEAN	TRUE	Delete landing files
gdpr_supported	BOOLEAN	TRUE	Can process GDPR requests
enabled	BOOLEAN	TRUE	Policy active
created_at	TIMESTAMP		Audit
updated_at	TIMESTAMP		Audit

Example:

policy_name	retention_days	delete_strategy
bronze_default	180	HARD_DELETE
customer_master	3650	TOMBSTONE
telemetry	30	HARD_DELETE
2. lifecycle_table_config

Instead of repeating values, tables reference a policy.

catalog	schema	table	layer	policy_name	deletion_column	business_key	contains_pii
prd	bronze	orders	bronze	bronze_default	ingestion_time	order_id	FALSE
prd	silver	customer	silver	customer_master	valid_to	customer_id	TRUE

Now changing one policy updates hundreds of tables.

How the Retention Job works

Instead of:

for every table
    retention_days = table.retention_days

it becomes:

SELECT
    table_name,
    deletion_column,
    retention_days,
    delete_strategy,
    grace_days
FROM lifecycle_table_config t
JOIN lifecycle_policy p
ON t.policy_name = p.policy_name

The engine is completely generic.

Why this matters

Imagine you have:

400 Bronze tables
700 Silver tables
120 Gold tables

If legal changes Bronze retention from 180 → 365 days, you update one row:

Policy

bronze_default

180

↓

365

No pipeline changes.

No table updates.

I would even add one more concept

For very large organizations, I'd introduce a policy type.

policy_name	policy_type
bronze_default	RETENTION
customer_master	RETENTION
legal_hold	LEGAL_HOLD
never_delete	EXEMPT

Then the lifecycle engine becomes extensible.

For example:

Table
↓

Policy

↓

Retention

↓

GDPR

↓

Legal Hold

↓

Delete?

A GDPR request could be blocked automatically if a Legal Hold policy applies.

Final metadata model

I think this is the metadata model I'd use for an enterprise Lakeflow SDP platform:

metadata.lifecycle_policy          ← reusable policies
metadata.lifecycle_table_config    ← tables using policies
metadata.entity_mapping            ← GDPR lookup
metadata.deletion_requests         ← incoming GDPR requests
metadata.ingestion_watermark       ← replay protection
metadata.lifecycle_audit           ← every delete performed

This separation follows a common enterprise pattern:

Policy = business rules.
Configuration = where those rules apply.
Execution = lifecycle engine.
Audit = immutable evidence of what happened.

That is much more scalable than putting all retention parameters directly into a single configuration table, and it's the approach I would expect on a platform supporting thousands of tables across multiple domains.
