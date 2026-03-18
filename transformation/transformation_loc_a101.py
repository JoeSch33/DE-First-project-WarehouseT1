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

df = pd.read_sql_query("SELECT * FROM ingestion.loc_a101;", con=conn)

#--------------------------
# DATA CLEANING STEPS
#--------------------------
# Turn empty strings / spaces into real missing values
df = df.replace(r"^\s*$", pd.NA, regex=True)

# Fix country names and fill missing values with "NA"
df["CNTRY"] = df["CNTRY"].replace({
    "USA": "United States",
    "US": "United States",
    "DE": "Germany"
})

df["CNTRY"] = df["CNTRY"].fillna("NA")

# Remove dashes from CID and fill missing values with "NA"
df["CID"] = df["CID"].fillna("NA")
df["CID"] = df["CID"].str.replace("-", "", regex=False)


#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/data_warehouse"
)

df.to_sql(
    name="loc_a101",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into transformation.loc_a101")

cur.close()
conn.close()
