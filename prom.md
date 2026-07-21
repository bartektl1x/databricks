I would like to design concept of data deletion in SPD pipelines from bronze to silver to gold. My current architecture bronze (streaming SPD) > dq (streaming SPD) > silver (rdv scd2 streaming SPD with cdc) > gold (materialized view). I would like to have some kind of configuration table with expiration data when data should be deleted but also prepare for future GDPR compliance. Remember I am using databricks streaming SDP. Ideally look for some concept already designed by someone smart. It sounds like normal and challenge of EVERY big databricks user so should already be well handled and though throught.



"There is a fundamental architectural tension: Auto-TTL performs async DELETEs on Bronze streaming tables, which appear as non-append commits. Downstream streaming tables must use skipChangeCommits=true to survive these commits, but this makes them blind to GDPR delete events. The solution requires a dual-path design: Auto-TTL for retention (with skipChangeCommits on downstream readers) AND a separate CDF-based propagation path (readChangeFeed=true + apply_changes with apply_as_deletes) for GDPR RTBF deletes. SCD Type 1 performs physical deletion (GDPR compliant); SCD Type 2 only tombstones (not compliant). Materialized Views auto-handle upstream deletes but do not support Auto-TTL."



Design a production data deletion architecture for our Lakeflow Pipelines (DLT) medallion platform: Bronze (streaming tables, AutoLoader) → Silver (Data Vault RDV: Hubs/Links/Sats) → Gold (Materialized Views). Must support both retention (Auto-TTL) and GDPR RTBF without breaking streams.

CONFIGURATION (2 tables only):

1. deletion_policy — human-managed rules with scope_pattern (SQL LIKE, e.g. '%.bronze.%', '%.silver.hub_%') and priority (int, higher = more specific). Columns: policy_id, scope_pattern, priority, retention_days, time_column, auto_ttl_enabled, gdpr_eligible, gdpr_delete_mode, gdpr_key_column, scd_type, skip_change_commits, cdf_enabled. Why: pattern inheritance means 10 policies cover 1000+ tables; exact-match overrides use high priority.

2. table_deletion_effective — machine-resolved cache. Columns: catalog, schema, table, effective_policy_id, plus denormalized config columns (retention_days, gdpr_eligible, etc.). Why: pipelines do O(1) lookup at startup; zero pattern matching at runtime.

ARCHITECTURE REQUIREMENTS:
- Bronze streaming tables use Auto-TTL for retention. Downstream Silver streaming reads must use skipChangeCommits=true to survive Auto-TTL async deletes.
- GDPR RTBF uses a separate path: Bronze CDF (readChangeFeed=true) feeds Silver apply_changes with apply_as_deletes. 
- Silver Hubs/Links: SCD Type 1 (physical row delete, GDPR compliant).
- Silver Satellites: SCD Type 2 for history, but GDPR requires post-process REORG PURGE hard-delete job.
- Gold MVs: auto-refresh handles upstream deletes. No Auto-TTL on MVs — use batch purge for Gold aggregates if needed.
- Physical cleanup sequence: DELETE → REORG TABLE ... APPLY (PURGE) → VACUUM.

OUTPUT:
- Mermaid architecture diagram showing both Auto-TTL and GDPR paths
- Exact DDL for the 2 config tables
- PySpark resolution engine (MERGE into table_deletion_effective)
- DLT pipeline code: Bronze (Auto-TTL + CDF view), Silver apply_changes for Hub/Link/Sat, Gold MV
- GDPR orchestrator job that reads config, executes CASCADE/NULLIFY, runs REORG+VACUUM, writes audit log
