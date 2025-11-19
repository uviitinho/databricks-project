# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from tqdm import tqdm

fe = FeatureEngineeringClient()

def import_query(path):
    with open(path) as open_file:
        return open_file.read()


def table_exists(catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                  .filter(f"tableName='{table}'")
                  .count())
    return count > 0

# COMMAND ----------

def table_exists(database, schema, table):
    return spark.catalog.tableExists(f"{database}.{schema}.{table}")

catalog = "feature_store"
database = "upsell"
table = "fs_geral"
tabblename = f"{catalog}.{database}.{table}"

query = import_query("fs_geral.sql")

dates = [
    '2024-02-01',
    '2024-03-01',
    '2024-04-01',
    '2024-05-01',
    '2024-06-01',
    '2024-07-01'
]

if not table_exists("catalog", "database", "table"):
    print("Criando tabela...")
    
    df = spark.sql(query,args={"dtRef": dates.pop(0)}               
    ).withColumnRenamed("IdCliente", "idCliente")
    fe.create_table(
        name="tablename",
        primary_keys=['dtRef', "idCliente"],
        df=df,
        partition_columns=['dtRef'],
        schema=df.schema
    )

for d in tqdm(dates):
    df = spark.sql(
        query,
        args={"dtRef": d}
    ).withColumnRenamed("IdCliente", "idCliente")
    fe.write_table(
        name="feature_store.upsell.fs_geral",
        df=df,
        mode="merge"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM feature_store.upsell.fs_geral
# MAGIC ORDER BY dtRef
# MAGIC
