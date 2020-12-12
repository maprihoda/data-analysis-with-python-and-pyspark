# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import SparkSession

# works on Java 8, NOT 11 (setting Dio.netty.tryReflectionSetAccessible currently doesn't work)
# sdk install java 8.0.275.hs-adpt

# https://github.com/GoogleCloudDataproc/spark-bigquery-connector#using-in-jupyter-notebooks
spark = (
    SparkSession
        .builder
        .config("spark.driver.memory", "8g")
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.0"
        )
        .getOrCreate()
)

# OK
# /home/marek/.ivy2/jars/com.google.cloud.spark_spark-bigquery-with-dependencies_2.12-0.18.0.jar

# %%
from functools import reduce
from pyspark.sql import DataFrame

bq_key = os.path.join(os.environ["HOME"], "Documents", "spark", "bq-key.json")

def read_df_from_bq(year):
    return (
        spark.read.format("bigquery")
        .option("table", f"bigquery-public-data.noaa_gsod.gsod{year}")
        .option("credentialsFile", bq_key)
        .option("spark.sql.execution.arrow.pyspark.enabled", "true")
        .option("parentProject", "pyspark-297919")
        .option("fetchsize", "10000")
        .load()
    )

# %%
gsod = (
    reduce(
        # DataFrame.union, [read_df_from_bq(year) for year in range(2018, 2019)]
        DataFrame.union, [read_df_from_bq(year) for year in [2018]]
    )
    .dropna(subset=["year", "mo", "da", "temp"])
    .where(F.col("temp") != 9999.9)
)

# %%
gsod.select("stn", "year", "mo", "da", "temp").orderBy("stn", "year", "mo", "da").show(50, False)

# %%
gsod_alt = read_df_from_bq(2010)
for year in range(2011, 2019):
    gsod_alt = gsod_alt.union(read_df_from_bq(year))

gsod_alt.show(1)

# %%
import pandas as pd

@F.pandas_udf(T.DoubleType(), F.PandasUDFType.SCALAR)
def f_to_c(degrees):
    """Transforms Farhenheit to Celcius."""
    return (degrees - 32) * 5 / 9


f_to_c.func(pd.Series(range(32, 213)))

# %%
gsod = gsod.withColumn("temp_c", f_to_c(F.col("temp")))
gsod.select("temp", "temp_c").distinct().show(5)

# %%
@F.pandas_udf(
    T.StructType(
        [
            T.StructField("stn", T.StringType()),
            T.StructField("year", T.StringType()),
            T.StructField("mo", T.StringType()),
            T.StructField("da", T.StringType()),
            T.StructField("temp", T.DoubleType()),
            T.StructField("temp_norm", T.DoubleType()),
        ]
    ),
    F.PandasUDFType.GROUPED_MAP,
)
def scale_temperature(temp_by_day):
    """Returns a simple normalization of the temperature for a site.

    If the temperature is constant for the whole window, defaults to 0.5."""
    temp = temp_by_day.temp
    answer = temp_by_day[["stn", "year", "mo", "da", "temp"]]
    if temp.min() == temp.max():
        return answer.assign(temp_norm=0.5)
    return answer.assign(temp_norm=(temp - temp.min()) / (temp.max() - temp.min()))

# %%
gsod = gsod.where(F.col("year") == "2018")

# %%
# Exercise 8.4
gsod_exo = gsod.groupby("year", "mo").apply(scale_temperature)
gsod_exo.show(5, False)
# normalizes temperatures for a given year/month accross all stations

# %%
# apply works on unaggragated grouped result sets (GROUP BY before applying aggregation); stn, year, mo will contain same values
gsod = gsod.groupby("stn", "year", "mo").apply(scale_temperature)

gsod.show(5, False)

# %%
rdd = gsod.take(50)
gsod = spark.createDataFrame(rdd)
gsod.show(1)

# %%
from sklearn.linear_model import LinearRegression

@F.pandas_udf(T.DoubleType(), F.PandasUDFType.GROUPED_AGG)
def rate_of_change_temperature(day, temp):
    """Returns the slope of the daily temperature for a given period of time."""
    return (
        LinearRegression()
            .fit(X=day.astype("int").values.reshape(-1, 1), y=temp)
            .coef_[0]
    )

# %%
result = gsod.groupby("stn", "year", "mo").agg(
    rate_of_change_temperature(gsod["da"], gsod["temp_norm"]).alias(
        "rt_chg_temp"
    )
)

result.show(5, False)

# %%
result.groupby("stn").agg(
    F.sum(F.when(F.col("rt_chg_temp") > 0, 1).otherwise(0)).alias("temp_increasing"),
    F.count("rt_chg_temp").alias("count"),
).where(F.col("count") > 6).select(
    F.col("stn"),
    (F.col("temp_increasing") / F.col("count")).alias("temp_increasing_ratio"),
).orderBy(
    "temp_increasing_ratio"
).show(5, False)

# %%

gsod_local = gsod.where("year = '2018' and mo = '01' and stn = '997183'").toPandas()

print(rate_of_change_temperature.func(gsod_local["da"], gsod_local["temp_norm"]))
