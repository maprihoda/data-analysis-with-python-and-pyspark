# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession
        .builder
        .config("spark.driver.memory", "8g")
        .config(
            "spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.0"
        )
        .getOrCreate()
)

# %%
bq_key = os.path.join(os.environ["HOME"], "Documents", "spark", "bq-key.json")

def read_df_from_bq(year):
    return (
        spark.read.format("bigquery")
        .option("table", f"bigquery-public-data.noaa_gsod.gsod{year}")
        .option("credentialsFile", bq_key)
        .option("spark.sql.execution.arrow.pyspark.enabled", "true")
        .option("parentProject", "pyspark-297919")
        .option("fetchsize", "10000")
        .load()
    )

# %%
gsod2018 = read_df_from_bq(2018).cache()
gsod2019 = read_df_from_bq(2019).cache()

# %%
gsod2018.show()
gsod2019.show()

# %%
gsod2018.join(gsod2019, "stn").explain()
