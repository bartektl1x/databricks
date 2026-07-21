-- Resolution logic: for every table, pick the matching policy with highest priority
MERGE INTO platform_config.table_deletion_effective tgt
USING (
    SELECT 
        t.table_catalog AS catalog,
        t.table_schema  AS schema_name,
        t.table_name    AS table_name,
        p.policy_id     AS effective_policy_id,
        p.retention_days,
        p.time_column,
        p.auto_ttl_enabled,
        p.gdpr_eligible,
        p.gdpr_delete_mode,
        p.gdpr_key_column,
        p.scd_type,
        p.skip_change_commits,
        p.cdf_enabled
    FROM system.information_schema.tables t
    JOIN platform_config.deletion_policy p
        ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) LIKE p.scope_pattern
    WHERE t.table_type IN ('MANAGED', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.table_catalog, t.table_schema, t.table_name
        ORDER BY p.priority DESC
    ) = 1
) src
ON tgt.catalog = src.catalog 
   AND tgt.schema_name = src.schema_name 
   AND tgt.table_name = src.table_name
WHEN MATCHED AND (
    tgt.effective_policy_id != src.effective_policy_id
    OR tgt.retention_days != src.retention_days
    OR tgt.gdpr_eligible != src.gdpr_eligible
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
