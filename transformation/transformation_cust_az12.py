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

df = pd.read_sql_query("SELECT * FROM ingestion.cust_az12;", con=conn)

#--------------------------
# DATA CLEANING STEPS
#--------------------------
# Turn empty strings / spaces into real missing values
df = df.replace(r"^\s*$", pd.NA, regex=True)

# strip whitespace so we can uniformly change gender
df["GEN"] = df["GEN"].str.strip()

df["GEN"] = df["GEN"].replace({
    "F": "Female",
    "M": "Male"
})

df["GEN"] = df["GEN"].fillna("NA")


# fix customer IDs by removing "NAS" and filling missing values with "NA"
df["CID"] = df["CID"].str.replace("NAS", "", regex=False)
df["CID"] = df["CID"].fillna("NA")





#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/data_warehouse"
)

df.to_sql(
    name="cust_az12",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into transformation.cust_az12") 

cur.close()
conn.close()

