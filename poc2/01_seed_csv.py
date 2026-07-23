"""Create deterministic CSV input for POC 2."""
from pathlib import Path
CATALOG = "main"
SCHEMA = "demo"
VOLUME = "poc2_source_files"
SOURCE_DIRECTORY = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/customers"
SOURCE_FILE = f"{SOURCE_DIRECTORY}/customers_001.csv"
CSV_CONTENT = """customer_id,name,email,city,updated_at
C001,Alice Johnson,alice@example.com,New York,2026-07-20T10:00:00Z
C002,Bob Smith,bob@example.com,Los Angeles,2026-07-20T11:00:00Z
C003,Charlie Brown,not-an-email,Chicago,2026-07-20T12:00:00Z
,Missing Identifier,missing.id@example.com,Warsaw,2026-07-20T13:00:00Z
C004,Diana Prince,diana@example.com,Miami,2026-07-20T14:00:00Z
"""
path=Path(SOURCE_DIRECTORY); path.mkdir(parents=True, exist_ok=True)
assert list(path.iterdir()) == [], "Run 00_setup_and_reset.py before reseeding."
Path(SOURCE_FILE).write_text(CSV_CONTENT, encoding="utf-8")
assert Path(SOURCE_FILE).exists()
print(f"Created {SOURCE_FILE}")
print(CSV_CONTENT)
