# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Getting the Canadian TV channels with the highest/lowest proportion of commercials."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")
DIRECTORY = "../data"

# %%
logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8-SAMPLE.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

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

call_signs = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/Call_Signs.csv"),
    sep=",",
    header=True,
    inferSchema=True
).select("LogIdentifierID", "Undertaking_Name")

# %%
logs = logs.drop("BroadcastLogID", "SequenceNO")

logs = logs.withColumn(
    "duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)

# log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

# %%
logs_and_channels = logs.join(log_identifier, "LogServiceID")
logs_and_channels.printSchema()

# %%
logs_and_channels = logs_and_channels.join(call_signs, "LogIdentifierID")
logs_and_channels.printSchema()

# %%
full_log = (
    logs_and_channels
        .join(cd_category, "CategoryID", how="left")
        .join(cd_program_class, "ProgramClassID", how="left"
))

full_log.printSchema()

# %%
# (
#     F.when([BOOLEAN TEST], [RESULT IF TRUE])
#         .when([ANOTHER BOOLEAN TEST], [RESULT IF TRUE])
#     .otherwise([DEFAULT RESULT, WILL DEFAULT TO null IF OMITTED])
# )

answer = (
    full_log
    .groupby("LogIdentifierID", "Undertaking_Name")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds"),
            ).when(
                F.trim(F.col("ProgramClassCD")) == "PRC",
                F.col("duration_seconds") * 0.75
            )
            .otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio",
        F.col("duration_commercial") / F.col("duration_total")
    )
    .fillna(0)
)

answer.orderBy("commercial_ratio", ascending=False).show(100, False)

# %%
answer.select("LogIdentifierID", "commercial_ratio").printSchema()

# %%
channels_count = answer.count()
channels_count

# %%
# first try
(answer
    .select("LogIdentifierID", "commercial_ratio")
    .groupBy(
        F.when(
            (F.col("commercial_ratio") < 1) & (F.col("commercial_ratio") >= 0.9),
            F.lit(0.9)
        ).when(
            (F.col("commercial_ratio") < 0.9) & (F.col("commercial_ratio") >= 0.8),
            F.lit(0.8)
        ).when(
            (F.col("commercial_ratio") < 0.8) & (F.col("commercial_ratio") >= 0.7),
            F.lit(0.7)
        ).when(
            (F.col("commercial_ratio") < 0.7) & (F.col("commercial_ratio") >= 0.6),
            F.lit(0.6)
        ).when(
            (F.col("commercial_ratio") < 0.6) & (F.col("commercial_ratio") >= 0.6),
            F.lit(0.5)
        ).when(
            (F.col("commercial_ratio") < 0.5) & (F.col("commercial_ratio") >= 0.4),
            F.lit(0.4)
        ).when(
            (F.col("commercial_ratio") < 0.4) & (F.col("commercial_ratio") >= 0.3),
            F.lit(0.3)
        ).when(
            (F.col("commercial_ratio") < 0.3) & (F.col("commercial_ratio") >= 0.2),
            F.lit(0.2)
        ).when(
            (F.col("commercial_ratio") < 0.2) & (F.col("commercial_ratio") >= 0.1),
            F.lit(0.1)
        ).when(
            F.col("commercial_ratio") < 0.1,
            F.lit(0.0)
        ).otherwise(F.lit(1)).alias("commercial_ratio")
    ).agg(
        F.count(F.col("LogIdentifierID")).alias("channels_count")
    ).withColumn(
        "proportion_of_channels",
        F.col("channels_count") / channels_count
    )
).show(100)

# %%

# %%
bounds = [((x + 1) * 0.1, x * 0.1) for x in range(0, 10)]
upper, lower = bounds.pop(0)

case_condition = F.when(
    (F.col("commercial_ratio") < upper) & (F.col("commercial_ratio") >= lower),
    F.lit(lower)
)

for upper, lower in bounds:
    case_condition = case_condition.when(
        (F.col("commercial_ratio") < upper) & (F.col("commercial_ratio") >= lower),
        F.lit(lower)
    )

case_condition

# %%
(answer
    .select("LogIdentifierID", "commercial_ratio")
    .groupBy(case_condition.otherwise(F.lit(1)).alias("commercial_ratio"))
    .agg(
        F.count(F.col("LogIdentifierID")).alias("channels_count")
    ).withColumn(
        "proportion_of_channels",
        F.col("channels_count") / channels_count
    )
).show(100)
