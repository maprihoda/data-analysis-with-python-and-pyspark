# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

DIRECTORY = "../data"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8-SAMPLE.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).withColumn(
    "duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)

logs.printSchema()

# %%
log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

log_identifier.printSchema()

# %%
log_identifier.show(5)

# %%
# log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)
# log_identifier.count()

# %%
logs_and_channels_verbose = logs.join(
    log_identifier, logs["LogServiceID"] == log_identifier["LogServiceID"]
)

logs_and_channels_verbose.printSchema() # LogServiceID appears two times in the schema

# %%
try:
    logs_and_channels_verbose.select("LogServiceID")
except AnalysisException as err:
    print(err)

# %%
# DO THIS
logs_and_channels = logs.join(log_identifier, "LogServiceID")

logs_and_channels.printSchema() # LogServiceID appears only once

# %%
logs_and_channels_verbose = logs.join(
    log_identifier, logs["LogServiceID"] == log_identifier["LogServiceID"]
)

logs_and_channels_verbose.drop(log_identifier["LogServiceID"]).select("LogServiceID")

# %%
logs_and_channels_verbose = logs.alias("left").join(
    log_identifier.alias("right"),
    logs["LogServiceID"] == log_identifier["LogServiceID"],
)

logs_and_channels_verbose.drop(F.col("right.LogServiceID")).select("LogServiceID")

# %%
# (logs["LogServiceID"] == log_identifier["LogServiceID"]) & (logs["left_col"] < log_identifier["right_col"])
# (logs["LogServiceID"] == log_identifier["LogServiceID"]) | (logs["left_col"] > log_identifier["right_col"])

# (left["col1"] == right["colA"]) & (left["col2"] > right["colB"]) & (left["col3"] != right["colC"])
# OR
# [left["col1"] == right["colA"], left["col2"] > right["colB"], left["col3"] != right["colC"]]
# %%
cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),
)

cd_program_class = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_ProgramClass.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)

full_log = (
    logs_and_channels
        .join(cd_category, "CategoryID", how="left")
        .join(cd_program_class, "ProgramClassID", how="left")
)

# %%
(full_log
    .groupBy("ProgramClassCD", "ProgramClass_Description")
    .agg(F.sum("duration_seconds").alias("duration_total"))
    .orderBy("duration_total", ascending=False)
    .show(5, False)
)

# %%
(full_log
    .groupBy("ProgramClassCD", "ProgramClass_Description")
    .agg({"duration_seconds": "sum"})
    .withColumnRenamed("sum(duration_seconds)", "duration_total")
    .orderBy("duration_total", ascending=False)
    .show(5, False)
)

# %%
answer = (
    full_log
        .groupBy("LogIdentifierID")
        .agg(
            F.sum(
                F.when(
                    F.trim(F.col("ProgramClassCD")).isin(
                        ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                    ),
                    F.col("duration_seconds"),
                ).otherwise(0)
            ).alias("duration_commercial"),
            F.sum("duration_seconds").alias("duration_total"),
        )
        .withColumn(
            "commercial_ratio", F.col("duration_commercial") / F.col("duration_total")
        )
)

answer.orderBy("commercial_ratio", ascending=False).show(20, False)

# %%
full_log.select(F.countDistinct("LogIdentifierID")).show()

# %%
answer_no_null = answer.dropna(subset=["commercial_ratio"])

answer_no_null.orderBy("commercial_ratio", ascending=False).show(20, False)

# %%
print(answer_no_null.count())

# %%
answer_no_null = answer.fillna(0)

answer_no_null.orderBy("commercial_ratio", ascending=False).show(20, False)

# %%
print(answer_no_null.count())

# %%
answer_no_null = answer.fillna(
    {"duration_commercial": 0, "duration_total": 0, "commercial_ratio": 0}
)

print(answer_no_null.count())

# %%
full_log.select("LogIdentifierID", "ProgramClassCD").show(5)
