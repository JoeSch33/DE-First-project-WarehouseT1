import os
import psycopg2
import csv

DB_NAME = "data_warehouse"
SCHEMA = "ingestion"

CSV_FILES = [
    "cust_info.csv",
    "CUST_AZ12.csv",
    "LOC_A101.csv",
    "prd_info.csv",
    "PX_CAT_G1V2.csv",
    "sales_details.csv",
]

def table_name_from_file(filename: str) -> str:
    # "CUST_AZ12.csv" -> "cust_az12"
    return os.path.splitext(filename)[0].lower()

conn = psycopg2.connect(
    host="127.0.0.1",
    port="5432",
    database=DB_NAME,
    user="postgres",
    password="Password"
)

cur = conn.cursor()

# Make sure ingestion schema exists
cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";')
conn.commit()

for csv_path in CSV_FILES:
    table = table_name_from_file(csv_path)

    # Drop table if exists (fresh reload)
    cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}";')

    # Read CSV header
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)

    # Create table (all TEXT columns)
    columns_sql = ", ".join([f'"{col}" TEXT' for col in header])
    cur.execute(f'CREATE TABLE "{SCHEMA}"."{table}" ({columns_sql});')
    conn.commit()

    # Insert rows (batch for speed)
    placeholders = ", ".join(["%s"] * len(header))
    insert_sql = f'INSERT INTO "{SCHEMA}"."{table}" VALUES ({placeholders});'

    batch = []
    BATCH_SIZE = 1000
    inserted = 0

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        for row in reader:
            # safety: pad/trim if row length differs
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                row = row[:len(header)]

            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                cur.executemany(insert_sql, batch)
                inserted += len(batch)
                batch = []

        if batch:
            cur.executemany(insert_sql, batch)
            inserted += len(batch)

    conn.commit()
    print(f"Loaded {inserted} rows into {SCHEMA}.{table}")

cur.close()
conn.close()
