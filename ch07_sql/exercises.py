# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
from functools import reduce

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

home_dir = os.environ["HOME"]
DATA_DIRECTORY = os.path.join(home_dir, "Documents", "spark", "data", "backblaze")

DATA_FILES = [
    "drive_stats_2019_Q1",
    "data_Q2_2019",
    "data_Q3_2019",
    "data_Q4_2019",
]

data = [
    spark.read.csv(DATA_DIRECTORY + "/" + file, header=True, inferSchema=True)
    for file in DATA_FILES
]

common_columns = list(
    reduce(lambda x, y: x.intersection(y), [set(df.columns) for df in data])
)

assert set(["model", "capacity_bytes", "date", "failure"]).issubset(
    set(common_columns)
)

full_data = reduce(
    lambda x, y: x.select(common_columns).union(y.select(common_columns)), data
)

# %%
def failure_rate(drive_stats):
    drive_days = drive_stats.groupby(F.col("model")).agg(
        F.count(F.col("*")).alias("drive_days")
    )

    failures = (
        drive_stats.where(F.col("failure") == 1)
        .groupby(F.col("model"))
        .agg(F.count(F.col("*")).alias("failures"))
    )

    answer = (
        drive_days.join(failures, on="model", how="inner")
        .withColumn("failure_rate", F.col("failures") / F.col("drive_days"))
        .orderBy(F.col("failure_rate").desc())
    )

    return answer


failure_rate(full_data).show(5)

# %%
full_data.createOrReplaceTempView("backblaze_stats_2019")

# %%
spark.sql(
    """
    SELECT
        drive_days_rs.model,
        failures / drive_days AS failure_rate
    FROM (
        SELECT
            model,
            count(*) AS drive_days
        FROM backblaze_stats_2019
        GROUP BY model) drive_days_rs
    INNER JOIN (
        SELECT
            model,
            count(*) AS failures
        FROM backblaze_stats_2019
        WHERE failure = 1
        GROUP BY model) failures_rs
    ON
        drive_days_rs.model = failures_rs.model
    ORDER BY 2 desc
    """
).show(5)

# %%
spark.sql(
    """
    WITH
    drive_days_rs as (
        SELECT
            model,
            count(*) AS drive_days
        FROM backblaze_stats_2019
        GROUP BY model),
    failures_rs as (
        SELECT
            model,
            count(*) AS failures
        FROM backblaze_stats_2019
        WHERE failure = 1
        GROUP BY model)
    SELECT
        drive_days_rs.model,
        failures / drive_days AS failure_rate
    FROM drive_days_rs
    INNER JOIN failures_rs
    ON
        drive_days_rs.model = failures_rs.model
    ORDER BY 2 desc
    """
).show(5)

# %%
drive_days = full_data.groupby(F.col("model")).agg(
    F.count(F.col("*")).alias("drive_days")
)

failures = (
    full_data.where(F.col("failure") == 1)
    .groupby(F.col("model"))
    .agg(F.count(F.col("*")).alias("failures"))
)

answer = (
    drive_days.join(failures, on="model", how="inner")
    .withColumn("failure_rate", F.col("failures") / F.col("drive_days"))
    .orderBy(F.col("failure_rate").desc())
)

answer = (
    drive_days.join(failures, on="model", how="inner")
    # .orderBy("drive_days", "failures", ascending=[False, True])
    .orderBy("drive_days", ascending=False)    # this is enough since models are already distinct
)

answer.show(50)

# %%
full_data.printSchema()

# %%
full_data = full_data.withColumn("date", F.col("date").cast(T.DateType()))
full_data.printSchema()

# %%
answer = (
    full_data
        .groupby(F.year("date").alias("year"), F.month("date").alias("month"))
        .agg(F.sum("capacity_bytes").alias("capacity_bytes"))
        .withColumn("capacity_gb", F.col("capacity_bytes") / F.pow(F.lit(1024), 3))
        .orderBy("year", "month")
)

answer.show(50)

# %%
full_data.groupby(F.col("model")).agg(F.count("capacity_bytes")).show(100)

# %%
(full_data
    .groupby(F.col("model"))
    .agg(F.count("capacity_bytes").alias("count_capacity_bytes"))
    .where("count_capacity_bytes > 1")  # HAVING in SQL
    ).count()

# %%
(full_data
    .groupby(F.col("model"))
    .agg(F.count("capacity_bytes").alias("count_capacity_bytes"))
    .where("count_capacity_bytes = 1")
    ).count()

# %%
full_data.where("capacity_bytes < 0").select("model", "capacity_bytes", "date").show(150)

# %%
subquery = (
    full_data
        .where("capacity_bytes > 0")
        .groupby(F.col("model"))
        .agg(F.max(F.col("capacity_bytes")).alias("capacity_bytes"))
)

subquery.show(5)

# %%
answer = (
    full_data.distinct() # this is not enough => SQL
        .join(subquery, on=["model", "capacity_bytes"])

)

answer.select(
    F.col("model"),
    (F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("capacity_GB"),
    F.col("date"),
    F.col("failure")).show(5)

# %%
answer.count()

# %%
full_data.createOrReplaceTempView("full_data")

# %%
answer_sql = spark.sql(
    """
    SELECT
        model,
        MAX(capacity_bytes) AS capacity_bytes
    FROM full_data
    WHERE capacity_bytes > 0
    GROUP BY model
    """
)

answer_sql.show(5)

# %%
answer_sql.count()

# %%
answer_sql = spark.sql(
    """
    WITH
    max_capacities_by_model
    AS
    (
        SELECT
            model,
            MAX(capacity_bytes) AS capacity_bytes
        FROM full_data
        WHERE capacity_bytes > 0
        GROUP BY model
    ),
    full_data_windowed
    (
        SELECT *,
			ROW_NUMBER () OVER
                (
                    PARTITION BY model
                    ORDER BY capacity_bytes DESC
                ) AS row_number
        FROM full_data
        WHERE capacity_bytes > 0
    )
    -- SELECT *
    SELECT
        max_capacities_by_model.model,
        max_capacities_by_model.capacity_bytes,
        date,
        failure
    FROM full_data_windowed
    INNER JOIN max_capacities_by_model
        ON full_data_windowed.model = max_capacities_by_model.model
        AND full_data_windowed.row_number = 1
    """
)

answer_sql.count()

# %%
answer_sql.show()

# %%
# This is enough to clean the data, no need to join the grouped subquery, since the windows function already sorts by capacity_bytes desc.

answer_sql = spark.sql(
    """
    WITH
    full_data_windowed
    AS
    (
        SELECT *,
			ROW_NUMBER () OVER
                (
                    PARTITION BY model
                    ORDER BY capacity_bytes DESC
                ) AS row_number
        FROM full_data
        WHERE capacity_bytes > 0
    )
    -- SELECT *
    SELECT
        model,
        capacity_bytes,
        date,
        failure,
        row_number
    FROM full_data_windowed
    WHERE row_number = 1
    """
)

answer_sql.show()

# %%
answer_sql.count()
