# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()


# %%
some_timestamps = spark.createDataFrame(
    [["2019-04-01"], ["2020-07-17"], ["1994-12-03"]],
    ["as_string"],
)

some_timestamps.printSchema()


# %%
some_timestamps = some_timestamps.withColumn("as_date", F.col("as_string").cast(T.DateType()))

some_timestamps.printSchema()


# %%
some_timestamps = some_timestamps.withColumn("as_timestamp", F.col("as_date").cast(T.TimestampType()))

some_timestamps.printSchema()


# %%
some_timestamps.show()


# %%
data = [
    ["1.0", "2020-04-07", "3"],
    ["1042,5", "2015-06-19", "17,042,174"],
    ["17.03.04178", "2019/12/25", "17_092"],
]

schema = T.StructType(
    [
        T.StructField("number_with_decimal", T.StringType()),
        T.StructField("dates_inconsistently_formatted", T.StringType()),
        T.StructField("integer_with_separators", T.StringType()),
    ]
)

cast_df = spark.createDataFrame(data, schema)

cast_df.show(3, False)


# %%
cast_df = cast_df.select(
    F.col("number_with_decimal")
        .cast(T.DoubleType())
        .alias("number_with_decimal"),
    F.col("dates_inconsistently_formatted")
        .cast(T.DateType())
        .alias("dates_inconsistently_formatted"),
    F.col("integer_with_separators")
        .cast(T.LongType())
        .alias("integer_with_separators"),
)

cast_df.show(3, False)


# %%
cast_df.fillna("2020, 1, 1", ["dates_inconsistently_formatted"]).show()


# %%
from datetime import date as d

cast_df = cast_df.withColumn(
    "dates_inconsistently_formatted",
    F.when(F.isnull(F.col("dates_inconsistently_formatted")), d(2020, 1, 1))
        .otherwise(F.col("dates_inconsistently_formatted"))
)

cast_df.show()


