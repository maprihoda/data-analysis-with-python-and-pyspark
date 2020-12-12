# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

DIRECTORY = "../data"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8-SAMPLE.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

# %%
columns_idless = [c for c in logs.columns if not c.endswith("ID")]
print(logs.columns)
# %%
print(columns_idless)

# %%
logs_clean = logs.select(*columns_idless)
logs_clean
