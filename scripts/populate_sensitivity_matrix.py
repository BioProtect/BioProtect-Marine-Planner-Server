#!/usr/bin/env python3
"""
Populate bioprotect.sensitivity_matrix from Kilkieran_Sens_Mat_Simple.xlsx

The CI function joins: sm.eunis_code = mif.alias AND sm.pressure = pressures.pressuretitle
So we store feature aliases (not EUNIS codes) in eunis_code column, and
pressure titles in the pressure column.

We insert rows for both the underscore-variant alias (e.g. "Deep_rocky_reef")
and the space-variant (e.g. "Deep rocky reef") to match whichever alias
format is used in a given project's features.
"""

import sys
import openpyxl
import psycopg2

EXCEL_PATH = "MVP/Kilkieran_Sens_Mat_Simple.xlsx"
DB_NAME = "bioprotect"

# Map Excel sensitivity text → numeric score
SENSITIVITY_MAP = {
    "High": 5,
    "Medium": 3,
    "Low": 1,
    "Not sensitive": 0,
    "No evidence (NEv)": 2,
    "Insufficient evidence (IEv)": 2,
    "Not assessed (NA)": 2,
    "Not relevant (NR)": 0,
}


def normalize_to_underscore(name):
    """Convert 'Deep rocky reef' → 'Deep_rocky_reef', handle special chars."""
    s = name.strip()
    s = s.replace(" ", "_")
    s = s.replace("-", "_")
    s = s.replace("(", "")
    s = s.replace(")", "")
    s = s.replace("ërl", "_erl")  # Maërl → Ma_erl
    return s


def main():
    wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True)
    ws = wb["Numeric Scores"]

    # Read all scored rows from the Numeric Scores sheet
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        original_name, simple_name, eunis_code, eunis_name, pressure, score = row[:6]
        if not simple_name or not pressure or score is None:
            continue
        # score is already numeric in this sheet
        try:
            score = float(score)
        except (ValueError, TypeError):
            continue

        rows.append((simple_name.strip(), pressure.strip(), score))

    print(f"Read {len(rows)} rows from Excel")

    # Get all known feature aliases from the DB
    conn = psycopg2.connect(dbname=DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT alias FROM bioprotect.metadata_interest_features;")
    db_aliases = {r[0] for r in cur.fetchall()}

    # Get all known pressure titles from the PAD
    cur.execute("SELECT DISTINCT pressuretitle FROM bioprotect.pad;")
    db_pressures = {r[0] for r in cur.fetchall()}

    # Build insertion set: for each Excel row, create entries for all
    # matching DB alias variants
    insert_data = []
    matched_features = set()
    matched_pressures = set()
    unmatched_pressures = set()

    for simple_name, pressure, score in rows:
        # Check pressure match
        if pressure not in db_pressures:
            unmatched_pressures.add(pressure)
            continue
        matched_pressures.add(pressure)

        # Find all DB aliases that match this simple name
        underscore_name = normalize_to_underscore(simple_name)

        aliases_to_insert = set()
        # Try exact match with spaces
        if simple_name in db_aliases:
            aliases_to_insert.add(simple_name)
        # Try underscore variant
        if underscore_name in db_aliases:
            aliases_to_insert.add(underscore_name)
        # Try lowercase underscore (some aliases are lowercase)
        lower_under = underscore_name.lower()
        if lower_under in db_aliases:
            aliases_to_insert.add(lower_under)

        if not aliases_to_insert:
            # Store with underscore name anyway - will match if feature is added later
            aliases_to_insert.add(underscore_name)

        for alias in aliases_to_insert:
            insert_data.append((alias, pressure, score))
            matched_features.add(alias)

    print(f"Matched pressures: {len(matched_pressures)}")
    if unmatched_pressures:
        print(f"Unmatched pressures ({len(unmatched_pressures)}):")
        for p in sorted(unmatched_pressures):
            print(f"  - {p}")
    print(f"Feature aliases to insert: {len(matched_features)}")
    print(f"Total rows to insert: {len(insert_data)}")

    # Clear and insert
    cur.execute("DELETE FROM bioprotect.sensitivity_matrix;")
    print(f"Cleared existing rows")

    inserted = 0
    skipped = 0
    for eunis_code, pressure, score in insert_data:
        try:
            cur.execute("""
                INSERT INTO bioprotect.sensitivity_matrix
                    (eunis_code, pressure, sensitivity_score)
                VALUES (%s, %s, %s)
                ON CONFLICT (eunis_code, pressure)
                DO UPDATE SET sensitivity_score = EXCLUDED.sensitivity_score;
            """, (eunis_code, pressure, score))
            inserted += 1
        except Exception as e:
            print(f"Error inserting ({eunis_code}, {pressure}): {e}")
            conn.rollback()
            skipped += 1

    conn.commit()
    print(f"\nInserted {inserted} rows, skipped {skipped}")

    # Verify
    cur.execute("SELECT COUNT(*) FROM bioprotect.sensitivity_matrix;")
    print(f"Total rows in sensitivity_matrix: {cur.fetchone()[0]}")

    cur.execute("""
        SELECT eunis_code, pressure, sensitivity_score
        FROM bioprotect.sensitivity_matrix
        ORDER BY eunis_code, pressure
        LIMIT 5;
    """)
    print("\nSample rows:")
    for r in cur.fetchall():
        print(f"  {r[0]} | {r[1]} | {r[2]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
