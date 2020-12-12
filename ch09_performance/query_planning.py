# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.stop()

# %%
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Launching PySpark with custom options")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
).getOrCreate()


spark.stop()

# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName(
        "Counting word occurences from a book, under a microscope."
    )
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

results = (
    spark.read.text("../data/pride-and-prejudice.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"),  r"(\W+)?([a-z]+)", 2).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
)

results.show(5, False)

# %%
results = (
    spark.read.text("../data/pride-and-prejudice.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"),  r"(\W+)?([a-z]+)", 2).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
    .where(F.length(F.col("word")) > 8)
    .groupby(F.length(F.col("word")))
    .sum("count")
)

results.show(5, False)

# %%
results.explain()

# %%
results_bis = (
    spark.read.text("../data/pride-and-prejudice.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"),  r"(\W+)?([a-z]+)", 2).alias("word"))
    .where(F.col("word") != "")
    .where(F.length(F.col("word")) > 8)
    .groupby(F.length(F.col("word")))
    .count()
)

results_bis.show(5, False)

# %%
results_bis.explain()
