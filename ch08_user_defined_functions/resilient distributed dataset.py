# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

collection = [1, "two", 3.0, ("four", 4), {"five": 5}]

sc = spark.sparkContext

collection_rdd = sc.parallelize(collection)

collection_rdd

collection_rdd.collect()

# %%
def add_one(value):
    return value + 1

collection_rdd = collection_rdd.map(add_one)

# WRONG
# print(collection_rdd.collect())

# %%
collection_rdd = sc.parallelize(collection)

def safer_add_one(value):
    try:
        return value + 1
    except TypeError:
        return value

collection_rdd = collection_rdd.map(safer_add_one)

collection_rdd.collect()

# %%
collection_rdd = collection_rdd.filter(lambda elem: isinstance(elem, (float, int)))

collection_rdd.collect()

# %%
from operator import add

collection_rdd = sc.parallelize([4, 7, 9, 1, 3])

collection_rdd.reduce(add)

# %%
df = spark.createDataFrame([[1], [2], [3]], schema=["column"])

df.rdd

df.rdd.collect()
