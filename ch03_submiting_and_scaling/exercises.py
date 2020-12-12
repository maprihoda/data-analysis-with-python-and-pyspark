# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName(
    "Counting word occurences from a book."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

result = (
    spark.read.text("../data/pride-and-prejudice.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), r"(\W+)?([a-z]+)", 2).alias("word"))
    .where(F.col("word") != "")
)

result.show(5)

# %%
result.select(F.countDistinct("word")).show()

# %%
result.distinct().count()

# %%
result.groupBy("word").count().orderBy("count", ascending=False).show()

# %%
result.groupBy("word").count().where(F.col("count") == 1).show(20)

# %%
(result
    .select(F.col("word")
    .substr(1, 1).alias("word"))
    .groupBy("word").count()
    .orderBy("count", ascending=False).show(5))

# %%
(result
    .select(
        F.col("word")
        .substr(1, 1).alias("word"))
    .where(
        F.col("word").isin(["a", "e", "i", "o", "u"])
    ).count())
