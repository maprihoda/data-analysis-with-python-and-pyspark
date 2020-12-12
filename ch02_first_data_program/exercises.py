# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, split, explode, lower, regexp_extract, greatest

spark = SparkSession.builder.getOrCreate()
book = spark.read.text("../data/pride-and-prejudice.txt")

# %%
ex21 = (book
            .select(
                length(col("value")).alias("number_of_chars")
            )
        )

ex21.show(10)

# %%
ex22 = spark.createDataFrame([('a', 1, 2)], ['key', 'value1', 'value2'])

ex22.select(
    "key", greatest(col("value1"), col("value2")).alias("maximum_value")
).select("key", "maximum_value").show()

# %%
lines = book.select(split(book.value, " ").alias("line"))

words = lines.select(explode(col("line")).alias("word"))

words_lower = words.select(lower(col("word")).alias("word_lower"))

words_clean = words_lower.select(
    regexp_extract(col("word_lower"),  r"(\W+)?([a-z]+)", 2).alias("word")
)

words_nonull = words_clean.where(col("word") != "")
words_nonull.show()

# %%
words_filtered = words_nonull.where(col("word") != "is")
words_filtered.show()

# %%
words_longer_than_3_chars = words_filtered.where(length("word") > 3)
words_longer_than_3_chars.show()

# %%
words_filtered = words_nonull.where(
    ~col("word").isin(["is", "it", "not", "the", "if", "a", "in", "of", "by", "and"])
)

words_filtered.show()

# %%
book.printSchema()

lines = book.select(split(book.value, " ").alias("line"))
words = lines.select(explode(col("line")).alias("word"))
words.show()
