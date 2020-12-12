# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = (
    SparkSession
        .builder.appName("Recipes ML model - Are you a dessert?")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
)

# https://www.kaggle.com/hugodarwood/epirecipes
home_dir = os.environ["HOME"]
DATA_DIRECTORY = os.path.join(home_dir, "Documents", "spark", "data", "recipes")

food = spark.read.csv(
    DATA_DIRECTORY + "/epi_r.csv", header=True, inferSchema=True
)

# %%
print(food.count(), len(food.columns))

# %%
food.printSchema()

# %%
def sanitize_column_name(name: str):
    """Drops unwanted characters from the column name.

    We replace spaces, dashes and slashes with underscore,
    and only keep alphanumeric characters."""

    answer = name

    for i, j in ((" ", "_"), ("-", "_"), ("/", "_"), ("&", "and")):
        answer = answer.replace(i, j)

    return "".join(
        char
        for char in answer
        if char.isalpha() or char.isdigit() or char == "_"
    )

# %%
food = food.toDF(*[sanitize_column_name(name) for name in food.columns])

for x in food.columns[:30]:
    food.select(x).summary().show()

# %%
import pandas as pd

pd.set_option("display.max_rows", 1000)

is_binary = food.agg(
    *[
        (F.size(F.collect_set(x)) == 2).alias(x)
        for x in food.columns
    ]
).toPandas()

is_binary.unstack()

# %%
print(is_binary.unstack()[~is_binary.unstack()])

# %%
food.agg(*[F.collect_set(x) for x in ("cakeweek", "wasteless")]).show(
    1, False
)

# %%
food.where("cakeweek > 1.0 or wasteless > 1.0").select(
    "title", "rating", "wasteless", "cakeweek", food.columns[-1]
).show()

# %%
food = food.where(
    (
        F.col("cakeweek").isin([0.0, 1.0])
        | F.col("cakeweek").isNull()
    )
    & (
        F.col("wasteless").isin([0.0, 1.0])
        | F.col("wasteless").isNull()
    )
)

print(food.count(), len(food.columns))

# %%
IDENTIFIERS = ["title"]

CONTINUOUS_COLUMNS = [
    "rating",
    "calories",
    "protein",
    "fat",
    "sodium",
]

TARGET_COLUMN = ["dessert"]

BINARY_COLUMNS = [
    x
    for x in food.columns
    if x not in CONTINUOUS_COLUMNS
    and x not in TARGET_COLUMN
    and x not in IDENTIFIERS
]

# %%
food = food.dropna(
    how="all",
    subset=[x for x in food.columns if x not in IDENTIFIERS],
)

food = food.dropna(subset=TARGET_COLUMN)

print(food.count(), len(food.columns))

# %%
food = food.fillna(0.0, subset=BINARY_COLUMNS)

print(food.where(F.col(BINARY_COLUMNS[0]).isNull()).count())

# %%
from typing import List, Optional

@F.udf(T.BooleanType())
def is_a_number(value: Optional[str]) -> bool:
    if not value:
        return True

    try:
        _ = float(value)
    except ValueError:
        return False

    return True


food.where(~is_a_number(F.col("rating"))).select(
    *CONTINUOUS_COLUMNS
).show()

# %%
for column in ["rating", "calories"]:
    food = food.where(is_a_number(F.col(column)))
    food = food.withColumn(column, F.col(column).cast(T.DoubleType()))

print(food.count(), len(food.columns))

# %%
food.select("rating", "calories", "protein", "fat", "sodium").summary(
    "mean", "stddev", "min", "1%", "5%", "50%", "95%", "99%", "max",
).show()

# %%
maximum = {
    "calories": 3203.0,
    "protein": 173.0,
    "fat": 207.0,
    "sodium": 5661.0,
}

for k, v in maximum.items():
    food = food.withColumn(k, F.least(F.col(k), F.lit(v)))


def compute_mean(df, include):
    return (
        df.agg(*(F.avg(c).alias(c) for c in include))
        .first()
        .asDict()
    )


computed_mean = compute_mean(
    food, ["calories", "protein", "fat", "sodium"]
)

food = food.fillna(computed_mean)

# %%
inst_sum_of_binary_columns = [
    F.sum(F.col(x)).alias(x) for x in BINARY_COLUMNS
]

sum_of_binary_columns = (
    food.select(*inst_sum_of_binary_columns).head().asDict()
)

sum_of_binary_columns

# %%
num_rows = food.count()

too_rare_features = [
    k
    for k, v in sum_of_binary_columns.items()
    if v < 10 or v > (num_rows - 10)
]

print(len(too_rare_features))

print(too_rare_features)

BINARY_COLUMNS = list(set(BINARY_COLUMNS) - set(too_rare_features))

# %%
food = food.withColumn(
    "protein_ratio", F.col("protein") * 4 / F.col("calories")
).withColumn(
    "fat_ratio", F.col("fat") * 9 / F.col("calories")
)

food = food.fillna(0.0, subset=["protein_ratio", "fat_ratio"])

CONTINUOUS_COLUMNS += ["protein_ratio", "fat_ratio"]

# %%
from pyspark.ml.feature import VectorAssembler

continuous_features = VectorAssembler(
    inputCols=CONTINUOUS_COLUMNS, outputCol="continuous_features"
)

continuous_features

# %%
vector_variable = continuous_features.transform(food)

vector_variable.select("continuous_features").show(3, False)

# %%
vector_variable.select("continuous_features").printSchema()

# %%
from pyspark.ml.stat import Correlation

correlation = Correlation.corr(
    continuous_features.transform(food), "continuous_features"
)

correlation.printSchema()

# %%
correlation_array = correlation.head()[0].toArray()
correlation_array

# %%

correlation_pd = pd.DataFrame(
    correlation_array,
    index=CONTINUOUS_COLUMNS,
    columns=CONTINUOUS_COLUMNS,
)

print(correlation_pd.iloc[:, :4])

# %%
print(correlation_pd.iloc[:, 4:])

# %%
from pyspark.ml.feature import MinMaxScaler

CONTINUOUS_NB = [x for x in CONTINUOUS_COLUMNS if "ratio" not in x]

continuous_assembler = VectorAssembler(
    inputCols=CONTINUOUS_NB, outputCol="continuous"
)

food_features = continuous_assembler.transform(food)

continuous_scaler = MinMaxScaler(
    inputCol="continuous", outputCol="continuous_scaled",
)

food_features = continuous_scaler.fit(food_features).transform(
    food_features
)

food_features.select("continuous_scaled").show(3, False)

# %%
model_binary_assembler = VectorAssembler(
    inputCols=BINARY_COLUMNS
    + ["continuous_scaled"]
    + ["protein_ratio", "fat_ratio"],
    outputCol="features",
)

food_model = model_binary_assembler.transform(food_features)

food_model.select("title", "dessert", "features").show(5, truncate=30)

# %%
print(food_model.schema["features"])

# %%
print(food_model.schema["features"].metadata)

# %%
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features", labelCol="dessert", predictionCol="prediction"
)

train, test = food_model.randomSplit([0.7, 0.3])

train.cache()

lrModel = lr.fit(train)

predictions = lrModel.transform(test)

# %%
predictions.select("prediction", "rawPrediction", "probability").show(
    3, False
)

# %%
from pyspark.mllib.evaluation import MulticlassMetrics

# Create (prediction, label) pairs
predictionAndLabel = predictions.select("prediction", "dessert").rdd

metrics = MulticlassMetrics(predictionAndLabel)

confusion_matrix = pd.DataFrame(
    metrics.confusionMatrix().toArray(),
    index=["dessert=0", "dessert=1"],
    columns=["predicted=0", "predicted=1"],
)

print(confusion_matrix)

# %%
print(f"Model precision: {metrics.precision(1.0)}")
print(f"Model recall: {metrics.recall(1.0)}")
print(f"Model f1-score: {metrics.fMeasure(1.0)}")

# %%
from pyspark.ml.evaluation import BinaryClassificationEvaluator

trainingSummary = lrModel.summary

evaluator = BinaryClassificationEvaluator(
    labelCol="dessert",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC",
)

accuracy = evaluator.evaluate(predictions)
print(f"Area under ROC = {accuracy} ")

# %%
import matplotlib.pyplot as plt

plt.figure(figsize=(5, 5))
plt.plot([0, 1], [0, 1], "r--")
plt.plot(
    lrModel.summary.roc.select("FPR").collect(),
    lrModel.summary.roc.select("TPR").collect(),
)
plt.xlabel("False positive rate")
plt.ylabel("True positive rate")
plt.show()

# %%
print(food_model.schema)

# %%
feature_names = ["(Intercept)"] + [
    x["name"]
    for x in (
        food_model
        .schema["features"]
        .metadata["ml_attr"]["attrs"]["numeric"]
    )
]

feature_coefficients = [lrModel.intercept] + list(
    lrModel.coefficients.values
)


coefficients = pd.DataFrame(
    feature_coefficients, index=feature_names, columns=["coef"]
)

coefficients["abs_coef"] = coefficients["coef"].abs()

print(coefficients.sort_values(["abs_coef"]))

# %%
for column in ["sorbet", "dorie_greenspan"]:
    food.where(F.col(column) == 1.0).select(
        "title", "dessert", column
    ).show(10, False)
