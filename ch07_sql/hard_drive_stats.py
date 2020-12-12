# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os

home_dir = os.environ["HOME"]
DATA_DIRECTORY = os.path.join(home_dir, "Documents", "spark", "data", "backblaze")


# %%
# uncomment and run
# ! python download_backblaze_data.py min {DATA_DIRECTORY}
# ! python download_backblaze_data.py full {DATA_DIRECTORY}

# %%
# uncomment and run
# ! unzip {out + "/*.zip"} -d {out}

# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

q1 = spark.read.csv(
    DATA_DIRECTORY + "/drive_stats_2019_Q1", header=True, inferSchema=True
)

q1.printSchema()

# %%
q2 = spark.read.csv(
    DATA_DIRECTORY + "/data_Q2_2019", header=True, inferSchema=True
)

q2.printSchema()

# %%
q3 = spark.read.csv(
    DATA_DIRECTORY + "/data_Q3_2019", header=True, inferSchema=True
)

q3.printSchema()

# %%
q4 = spark.read.csv(
    DATA_DIRECTORY + "/data_Q4_2019", header=True, inferSchema=True
)

q4.printSchema()

# %%
# Q4 has two more fields than the rest
q4_fields_extra = set(q4.columns) - set(q1.columns)

for i in q4_fields_extra:
    q1 = q1.withColumn(i, F.lit(None).cast(T.StringType()))
    q2 = q2.withColumn(i, F.lit(None).cast(T.StringType()))
    q3 = q3.withColumn(i, F.lit(None).cast(T.StringType()))

# %%
# backblaze_2019 = q3

backblaze_2019 = (
    q1.select(q4.columns)
        .union(q2.select(q4.columns))
        .union(q3.select(q4.columns))
        .union(q4)
)

# %%
backblaze_2019 = backblaze_2019.select(
    *[
        F.col(x).cast(T.LongType()) if x.startswith("smart") else F.col(x)
        for x in backblaze_2019.columns
    ]
)

backblaze_2019.createOrReplaceTempView("backblaze_stats_2019")

# %%
spark.sql(
    "select serial_number from backblaze_stats_2019 where failure = 1"
).show(5)

# %%
backblaze_2019.where("failure = 1").select(F.col("serial_number")).show(5)

# %%

spark.sql(
    """SELECT
           model,
           min(capacity_bytes / pow(1024, 3)) min_GB,
           max(capacity_bytes/ pow(1024, 3)) max_GB
        FROM backblaze_stats_2019
        GROUP BY 1
        ORDER BY 3 DESC"""
).show(5)

# %%
spark.sql(
    """SELECT
           model,
           min(capacity_bytes) / pow(1024, 3) AS min_GB,
           max(capacity_bytes) / pow(1024, 3) AS max_GB
        FROM backblaze_stats_2019
        GROUP BY 1
        ORDER BY 3 DESC"""
).show(5)

# %%
backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).orderBy(F.col("max_GB"), ascending=False).show(5)

# %%
spark.sql(
    """SELECT
           model,
           min(capacity_bytes / pow(1024, 3)) min_GB,
           max(capacity_bytes/ pow(1024, 3)) max_GB
        FROM backblaze_stats_2019
        GROUP BY 1
        HAVING min_GB != max_GB
        ORDER BY 3 DESC"""
).show(5)

# %%
# NB: no HAVING in pyspark since each method returns a new data frame
backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).where(F.col("min_GB") != F.col("max_GB")).orderBy(
    F.col("max_GB"), ascending=False
).show(5)

# %%
spark.sql(
    """
    CREATE TEMP VIEW drive_days AS
        SELECT model, count(*) AS drive_days
        FROM backblaze_stats_2019
        GROUP BY model"""
)

spark.sql(
    """CREATE TEMP VIEW failures AS
           SELECT model, count(*) AS failures
           FROM backblaze_stats_2019
           WHERE failure = 1
           GROUP BY model"""
)

# %%
drive_days = backblaze_2019.groupby(F.col("model")).agg(
    F.count(F.col("*")).alias("drive_days")
)

failures = (
    backblaze_2019.where(F.col("failure") == 1)
    .groupby(F.col("model"))
    .agg(F.count(F.col("*")).alias("failures"))
)

# %%
# spark.sql("create table q1 as select * from csv.`../data/drive_stats_2019_Q1`")

# %% [markdown]
# PySpark’s union() ≠ SQL UNION
# use distinct() to remove the duplicates

# %%
columns_backblaze = ", ".join(q4.columns)

q1.createOrReplaceTempView("Q1")
q2.createOrReplaceTempView("Q2")
q3.createOrReplaceTempView("Q3")
q4.createOrReplaceTempView("Q4")

spark.sql(
    """
    CREATE TEMP VIEW backblaze_2019_view AS
    SELECT {col} FROM Q1 UNION ALL
    SELECT {col} FROM Q2 UNION ALL
    SELECT {col} FROM Q3 UNION ALL
    SELECT {col} FROM Q4
""".format(
        col=columns_backblaze
    )
)

# %%
backblaze_2019_view = (
    q1.select(q4.columns)
    .union(q2.select(q4.columns))
    .union(q3.select(q4.columns))
    .union(q4)
)

# %%
spark.sql(
    """select
           drive_days.model,
           drive_days,
           failures
    from drive_days
    left join failures
    on
        drive_days.model = failures.model"""
).show(5)

# %%
drive_days.join(failures, on="model", how="left").show(5)
