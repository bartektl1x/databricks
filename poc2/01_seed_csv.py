"""
Creates deterministic CSV input for POC 2.

Run outside Lakeflow pipelines after 00_setup_and_reset.py.

Expected DQX routing:
    C001 -> valid
    C002 -> valid
    C003 -> quarantine because email is invalid
    NULL -> quarantine because customer_id is missing
    C004 -> valid
"""

from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "dev_mr_dhc_bronze"
SCHEMA = "slpat_landing_staging"
VOLUME = "poc2_source_files"

SOURCE_DIRECTORY = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/customers"
SOURCE_FILE = f"{SOURCE_DIRECTORY}/customers_001.csv"


# =============================================================================
# DETERMINISTIC INPUT
# =============================================================================

CSV_CONTENT = """customer_id,name,email,city,updated_at
C001,Alice Johnson,alice@example.com,New York,2026-07-20T10:00:00Z
C002,Bob Smith,bob@example.com,Los Angeles,2026-07-20T11:00:00Z
C003,Charlie Brown,not-an-email,Chicago,2026-07-20T12:00:00Z
,Missing Identifier,missing.id@example.com,Warsaw,2026-07-20T13:00:00Z
C004,Diana Prince,diana@example.com,Miami,2026-07-20T14:00:00Z
"""

source_path = Path(SOURCE_DIRECTORY)
source_path.mkdir(parents=True, exist_ok=True)

existing_files = list(source_path.iterdir())
assert existing_files == [], (
    f"Expected an empty source directory before seeding, found {existing_files}. "
    "Run 00_setup_and_reset.py before reseeding."
)

Path(SOURCE_FILE).write_text(CSV_CONTENT, encoding="utf-8")

assert Path(SOURCE_FILE).exists(), f"Failed to create {SOURCE_FILE}."


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2 CSV CREATED")
print("=" * 80)
print(f"File: {SOURCE_FILE}")
print()
print(CSV_CONTENT)
print("Next step:")
print("Run a FULL REFRESH of the retention pipeline.")
print("=" * 80)
