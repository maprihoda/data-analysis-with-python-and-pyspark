# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# stop being lazy (no!)
# spark = (SparkSession.builder
#                      .config("spark.sql.repl.eagerEval.enabled", "True")
#                      .getOrCreate())

# %%
spark.sparkContext

# %%
sc = spark.sparkContext
sqlContext = spark

# %%
spark.sparkContext.setLogLevel("ERROR")

# %%
spark.read
dir(spark.read)

# spark.read.csv() === spark.read.format('csv').load()

# %%
spark.read?
spark.read??
print(spark.read.__doc__)

# %%
book = spark.read.text("../data/pride-and-prejudice.txt")

book

# %%
book.printSchema()

# %%
book.show()

# %%
book.show(10, truncate=50)

# %%
from pyspark.sql.functions import split

lines = book.select(split(book.value, " ").alias("line"))

lines.show(5)

# %%

# returns a dataframe
book.select(book.value)

# %%
from pyspark.sql.functions import col

book.select(book.value)
book.select(book["value"])
book.select(col("value"))
book.select("value")

# %%
from pyspark.sql.functions import split

lines = book.select(split(col("value"), " "))

lines

# %%
lines.printSchema()

# %%
lines.show(5)

# %%
book.select(split(col("value"), " ")).printSchema()

# %%
book.select(split(col("value"), " ").alias("line")).printSchema()

# %%
lines = book.select(split(book.value, " ").alias("line"))
lines.show(5)

# %%
# lines = book.select(split(book.value, " "))
# lines = lines.withColumnRenamed("split(value,  )", "line")

# %%
from pyspark.sql.functions import explode

words = lines.select(explode(col("line")).alias("word"))

words.show(20)

# %%
words.printSchema()

# %%
from pyspark.sql.functions import lower
words_lower = words.select(lower(col("word")).alias("word_lower"))

words_lower.show(100)

# %%
from pyspark.sql.functions import regexp_extract

words_clean = words_lower.select(
    # regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word")
    regexp_extract(col("word_lower"),  r"(\W+)?([a-z]+)", 2).alias("word")
)

words_clean.show(100)

# %%
words_nonull = words_clean.where(col("word") != "")

words_nonull.show(100)
