"""
Local static validation for the POC 2.1 source package.

This script uses only the Python standard library. It proves package
completeness, Python syntax, and the presence of critical architectural
contracts. It does not replace Databricks execution.
"""

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parent

REQUIRED_FILES = {
    "00_setup_and_reset.py",
    "01_seed_csv.py",
    "02_retention_pipeline.py",
    "03_cdc_marker_and_gold_pipeline.py",
    "04_assert_initial_state.py",
    "05_update_dqx_valid_row.py",
    "06_assert_update_propagation.py",
    "07_delete_dqx_valid_row.py",
    "08_assert_delete_propagation.py",
    "09_show_evidence.py",
    "ARCHITECTURE.md",
    "COMPLETION-AUDIT.md",
    "DATABRICKS-RUNBOOK.md",
    "README.md",
    "VALIDATION.md",
}

REQUIRED_PIPELINE_CONTRACTS = {
    "customer_hash_key",
    'lit("CUSTOMER")',
    'filter(col("_change_type") == "insert")',
    "HUB_TABLE",
    "stored_as_scd_type=1",
    "SILVER_TABLE",
    "stored_as_scd_type=2",
    'keys=["customer_hk"]',
    "track_history_column_list",
    "DELETE_MARKER_SOURCE_VIEW",
    '"customer_hk",\n        "delete_sequence"',
    "CLASSIFIED_VIEW",
    "closed_by_delete",
    "GOLD_VIEW",
}

REQUIRED_ASSERTION_CONTRACTS = {
    "Hub row count after source deletion",
    "C002 active Satellite versions after deletion",
    "C002 durable deletion-marker count",
    "Update-closed version classification",
    "Delete-closed version classification",
    "C002 Gold rows after deletion",
}

REQUIRED_ARCHITECTURE_CONTRACTS = {
    "source delete",
    "Retention expiration",
    "GDPR erasure",
    "pipelines.cdc.tombstoneGCThresholdInSeconds",
    "deletion marker",
    "full refresh",
}


def assert_contains_all(
    text: str,
    required_values: set[str],
    description: str,
) -> None:
    missing_values = sorted(
        value
        for value in required_values
        if value not in text
    )
    assert not missing_values, (
        f"{description} is missing required contracts: {missing_values}"
    )


actual_files = {
    path.name
    for path in ROOT.iterdir()
    if path.is_file()
}
missing_files = sorted(REQUIRED_FILES - actual_files)
assert not missing_files, f"Missing required POC files: {missing_files}"

parsed_python = {}
for path in sorted(ROOT.glob("*.py")):
    parsed_python[path.name] = ast.parse(
        path.read_text(encoding="utf-8"),
        filename=str(path),
    )

retention_pipeline_tree = parsed_python["02_retention_pipeline.py"]
active_auto_ttl_keywords = [
    keyword
    for node in ast.walk(retention_pipeline_tree)
    if isinstance(node, ast.Call)
    for keyword in node.keywords
    if keyword.arg == "auto_ttl"
]
assert not active_auto_ttl_keywords, (
    "POC 2.1 must not enable active Auto-TTL."
)

pipeline_text = (
    ROOT / "03_cdc_marker_and_gold_pipeline.py"
).read_text(encoding="utf-8")
assert_contains_all(
    pipeline_text,
    REQUIRED_PIPELINE_CONTRACTS,
    "Pipeline 2",
)

delete_assertion_text = (
    ROOT / "08_assert_delete_propagation.py"
).read_text(encoding="utf-8")
assert_contains_all(
    delete_assertion_text,
    REQUIRED_ASSERTION_CONTRACTS,
    "Delete assertion",
)

architecture_text = (
    ROOT / "ARCHITECTURE.md"
).read_text(encoding="utf-8")
assert_contains_all(
    architecture_text,
    REQUIRED_ARCHITECTURE_CONTRACTS,
    "Architecture documentation",
)

executable_text = "\n".join(
    path.read_text(encoding="utf-8")
    for path in sorted(ROOT.glob("*.py"))
    if path.name != Path(__file__).name
)

assert "table_changes(DQX_VALID_TABLE, 0)" not in executable_text
assert 'keys=["customer_id"]' not in pipeline_text
assert "is_deleted" not in pipeline_text

print("POC 2.1 source package validation passed.")
print(f"Validated {len(REQUIRED_FILES)} required files.")
print("All Python files parsed successfully.")
print("Critical Hub, Satellite, marker, classification, and Gold contracts found.")
print("Databricks runtime validation is still required.")
