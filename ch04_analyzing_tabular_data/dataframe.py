# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
import numpy as np
from pprint import pprint

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# %%
my_grocery_list = [
    ["Banana", 2, 1.74],
    ["Apple", 4, 2.04],
    ["Carrot", 1, 1.09],
    ["Cake", 1, 10.99],
]

df_grocery_list = spark.createDataFrame(my_grocery_list, ["Item", "Quantity", "Price"])

df_grocery_list.printSchema()

# %%
DIRECTORY = "../data"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8-SAMPLE.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

logs.printSchema()

# %%
# select expands the list anyway (see select's source code)
logs.select(*logs.columns[:3]).show(5, False)
logs.select(logs.columns[:3]).show(5, False)

# %%
logs.select("BroadCastLogID", "LogServiceID", "LogDate")

# %%
logs.select(F.col("BroadCastLogID"), F.col("LogServiceID"), F.col("LogDate"))

# %%
column_split = np.array_split(np.array(logs.columns), len(logs.columns) // 3)

pprint(column_split)

# %%
for ary in column_split:
    logs.select(*ary).show(5, False)

# %%
logs = logs.drop("BroadcastLogID", "SequenceNO")

# Testing if we effectively got rid of the columns
print("BroadcastLogID" in logs.columns)  # => False
print("SequenceNo" in logs.columns)  # => False

# %%
logs = logs.select(
    *[x for x in logs.columns if x not in ["BroadcastLogID", "SequenceNO"]]
)

logs.show(5)

# %%
logs.select(F.col("Duration")).show(5)

# %%
print(logs.select(F.col("Duration")).dtypes)

# %%
# the first character is 1
logs.select(
    F.col("Duration"),
    F.col("Duration").substr(1, 2).cast("int").alias("dur_hours"),
    F.col("Duration").substr(4, 2).cast("int").alias("dur_minutes"),
    F.col("Duration").substr(7, 2).cast("int").alias("dur_seconds"),
).distinct().show(5)

# %%
logs.select(
    F.col("Duration").substr(1, 2).cast("int").alias("dur_hours"),
    F.col("Duration").substr(4, 2).cast("int").alias("dur_minutes"),
    F.col("Duration").substr(7, 2).cast("int").alias("dur_seconds"),
).distinct().show(5)

# %%
logs.select(
    F.col("Duration"),
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ).alias("Duration_seconds"),
).distinct().show(5)

# %%
logs = logs.withColumn(
    "Duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)

logs.printSchema()

# %%
logs = logs.withColumnRenamed("Duration_seconds", "duration_seconds")

logs.printSchema()

# %%
logs.toDF(*[x.lower() for x in logs.columns]).printSchema()

# %%
logs.select(sorted(logs.columns, key=str.lower)).printSchema()

# %%
for i in logs.columns:
    logs.describe(i).show()

# %%
for i in logs.columns:
    logs.select(i).summary().show()

# %%
for i in logs.columns:
    logs.select(i).summary("min", "10%", "90%", "max").show()
