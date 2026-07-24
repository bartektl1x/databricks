"""
Create deterministic CSV input for POC 2.1.

Run 00_setup_and_reset.py first. This script intentionally refuses to add a
second file to a non-empty POC input directory.
"""

from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"
VOLUME = "poc21_source_files"

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

assert list(source_path.iterdir()) == [], (
    "The POC 2.1 source directory is not empty. "
    "Run 00_setup_and_reset.py before reseeding."
)

output_path = Path(SOURCE_FILE)
output_path.write_text(CSV_CONTENT, encoding="utf-8")

assert output_path.exists(), f"Expected source file {SOURCE_FILE}."


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2.1 SOURCE FILE CREATED")
print("=" * 80)
print(f"File: {SOURCE_FILE}")
print()
print(CSV_CONTENT)
print("Next: FULL REFRESH Pipeline 1, then FULL REFRESH Pipeline 2.")
print("=" * 80)

