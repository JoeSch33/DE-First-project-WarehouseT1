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

df = pd.read_sql_query("SELECT * FROM ingestion.px_cat_g1v2;", con=conn)

#--------------------------
# DATA CLEANING STEPS
#--------------------------
# Turn empty strings / spaces into real missing values
df = df.replace(r"^\s*$", pd.NA, regex=True)

#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/data_warehouse"
)

df.to_sql(
    name="px_cat_g1v2",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into transformation.px_cat_g1v2")

cur.close()
conn.close()
