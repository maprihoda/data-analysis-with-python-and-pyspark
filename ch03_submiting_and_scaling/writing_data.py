# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

results = (
    spark.read.text("../data/pride-and-prejudice.txt")
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), r"(\W+)?([a-z]+)", 2).alias("word"))
        .where(F.col("word") != "")
        .groupBy("word")
        .count()
        .orderBy("count", ascending=False)
)

results.show(10)

# %%
results.coalesce(1).write.csv("./results_single_partition.csv", mode="overwrite")

# %%
! ls -l

! ls -l results_single_partition.csv/