import psycopg2
import pandas as pd
from sqlalchemy import create_engine

#--------------------------
# READ SQL
#--------------------------
conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="data_warehouse",
    user="postgres",
    password="aaaa"
)

cur = conn.cursor()

df = pd.read_sql_query("SELECT * FROM ingestion.prd_info;", con=conn)

#--------------------------
# DATA CLEANING STEPS
#--------------------------
# Turn empty strings / spaces into real missing values
df = df.replace(r"^\s*$", pd.NA, regex=True)

# Clean prd_key and create cat_id
df["prd_key"] = df["prd_key"].astype("string").str.strip()
df["cat_id"] = df["prd_key"].str[:5]
df["prd_key"] = df["prd_key"].str[6:]
df["cat_id"] = df["cat_id"].str.replace("-", "_", regex=False)

# Clean prd_cost and prd_line first
df["prd_cost"] = df["prd_cost"].astype("string").str.strip()
df["prd_line"] = df["prd_line"].astype("string").str.strip()
df["prd_end_dt"] = df["prd_end_dt"].astype("string").str.strip()

# Fill missing values
df["prd_cost"] = df["prd_cost"].fillna("0")
df["prd_line"] = df["prd_line"].fillna("NA")
df["prd_end_dt"] = df["prd_end_dt"].fillna("NA")

# PRODUCTION DATE ISSUE:
df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"], errors="coerce")
df["prd_end_dt"] = pd.to_datetime(df["prd_end_dt"], errors="coerce")

df = df.sort_values(by=["prd_key", "prd_id"]).copy()

df["prd_end_dt"] = (
    df.groupby("prd_key")["prd_start_dt"].shift(-1) - pd.Timedelta(days=1)
)

df = df.sort_values(by=["prd_id"]).copy()
df["prd_end_dt"] = df["prd_end_dt"].dt.strftime("%Y-%m-%d")
df["prd_end_dt"] = df["prd_end_dt"].fillna("NA")



# Replace line codes
df["prd_line"] = df["prd_line"].replace({
    "R": "Road",
    "T": "Touring",
    "M": "Mountain",
    "S": "Sport"
})

#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/data_warehouse"
)

df.to_sql(
    name="prd_info",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into transformation.prd_info")

cur.close()
conn.close()

print("Cleaned data loaded into transformation.prd_info")

cur.close()
conn.close()
