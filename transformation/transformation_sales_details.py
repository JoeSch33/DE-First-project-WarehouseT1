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

df = pd.read_sql_query("SELECT * FROM ingestion.sales_details;", con=conn)

#--------------------------
# DATA CLEANING STEPS
#--------------------------
# Turn empty strings / spaces into real missing values
df = df.replace(r"^\s*$", pd.NA, regex=True)

#FIXING DATE:
# replace bad values with missing
df["sls_order_dt"] = df["sls_order_dt"].replace(["0", ""], pd.NA)
# convert text to real date
df["sls_order_dt"] = pd.to_datetime(df["sls_order_dt"], format="%Y%m%d", errors="coerce")

#do the same for ship and delivery dates
df["sls_ship_dt"] = df["sls_ship_dt"].replace(["0", ""], pd.NA)
df["sls_ship_dt"] = pd.to_datetime(df["sls_ship_dt"], format="%Y%m%d", errors="coerce")

df["sls_due_dt"] = df["sls_due_dt"].replace(["0", ""], pd.NA)
df["sls_due_dt"] = pd.to_datetime(df["sls_due_dt"], format="%Y%m%d", errors="coerce")



# make order dates consistent within each order number (null if only single item in order and wrong order date) 
def fix_order_dates(group):
    valid_dates = group["sls_order_dt"].dropna()

    if len(group) > 1:
        if len(valid_dates) > 0:
            group["sls_order_dt"] = valid_dates.min()
        else:
            group["sls_order_dt"] = pd.NaT
    else:
        if len(valid_dates) == 0:
            group["sls_order_dt"] = pd.NaT

    return group

df = df.groupby("sls_ord_num", group_keys=False).apply(fix_order_dates)

# FIXING SALES AND PRICES (we assume quantity is correct)

# convert to numeric first
df["sls_sales"] = pd.to_numeric(df["sls_sales"], errors="coerce")
df["sls_price"] = pd.to_numeric(df["sls_price"], errors="coerce")
df["sls_quantity"] = pd.to_numeric(df["sls_quantity"], errors="coerce")

# make negative values positive
df["sls_sales"] = df["sls_sales"].abs()
df["sls_price"] = df["sls_price"].abs()

# if price is missing or 0 -> calculate it from sales / quantity
df.loc[
    (df["sls_price"].isna() | (df["sls_price"] == 0)) &
    df["sls_sales"].notna() &
    df["sls_quantity"].notna() &
    (df["sls_quantity"] != 0),
    "sls_price"
] = df["sls_sales"] / df["sls_quantity"]

# then make sales equal price * quantity
df.loc[
    df["sls_price"].notna() & df["sls_quantity"].notna(),
    "sls_sales"
] = df["sls_price"] * df["sls_quantity"]

#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/data_warehouse"
)

df.to_sql(
    name="sales_details",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into transformation.sales_details")

cur.close()
conn.close()
