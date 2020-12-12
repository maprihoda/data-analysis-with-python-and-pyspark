# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

elements = spark.read.csv(
    "../data/Periodic_Table_Of_Elements.csv",
    header=True,
    inferSchema=True,
)

elements.where(F.col("phase") == "liq").groupBy("period").count().show()

# %%
try:
    spark.sql(
        "select period, count(*) from elements where phase='liq' group by period"
    ).show(5)
except AnalysisException as e:
    print(e)

# %%
elements.createOrReplaceTempView("elements")

spark.sql(
    "select period, count(*) from elements where phase='liq' group by period"
).show(5)

# %%
spark.catalog

# %%
spark.catalog.listTables()

# %%
spark.catalog.dropTempView("elements")

spark.catalog.listTables()
