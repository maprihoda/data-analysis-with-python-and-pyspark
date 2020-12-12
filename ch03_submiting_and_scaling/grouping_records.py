# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, regexp_extract

spark = SparkSession.builder.getOrCreate()

book = spark.read.text("../data/pride-and-prejudice.txt")

lines = book.select(split(book.value, " ").alias("line"))

words = lines.select(explode(col("line")).alias("word"))

words_lower = words.select(lower(col("word")).alias("word_lower"))

words_clean = words_lower.select(
    regexp_extract(col("word_lower"),  r"(\W+)?([a-z]+)", 2).alias("word")
)

words_nonull = words_clean.where(col("word") != "")
words_nonull.show()

# %%
groups = words_nonull.groupBy(col("word"))

groups

# %%
results = words_nonull.groupBy(col("word")).count()

results

# %%
results.show()

# %%
results.orderBy("count", ascending=False).show(10)
