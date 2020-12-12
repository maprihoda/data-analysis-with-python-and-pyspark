# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

sample_frame = spark.read.csv(
    "data/sample_frame.csv", inferSchema=True, header=True
)

sample_frame.show()


# %%
sample_frame.printSchema()


# %%
import pyspark.sql.functions as F

fruits = [
    ["Apple", "Pomme", "яблоко"],
    ["Banana", "Banane", "Банан"],
    ["Grape", "Raisin", "виноград"],
]

df = spark.createDataFrame(fruits, ["English", "French", "Russian"])

df.printSchema()


# %%
df.show()


# %%
df = df.withColumn(
    "altogether", F.concat(F.col("English"), F.col("French"), F.col("Russian"))
)

df = df.withColumn(
    "binary_encoded", F.encode(F.col("altogether"), "UTF-8")
)

df.select("altogether", "binary_encoded").show(10, False)


# %%
df.withColumn(
    "binary_encoded", F.encode(F.col("altogether"), "UTF-8")
).withColumn(
    "length_string", F.length(F.col("altogether"))
).withColumn(
    "length_binary", F.length(F.col("binary_encoded"))
).select(
    "altogether", "binary_encoded", "length_string", "length_binary"
).show()


# %%
short_values = [
    [404, 1926],
    [530, 2047]
]
columns = ["columnA", "columnB"]

short_df = spark.createDataFrame(short_values, columns)

short_df = short_df.select(
    *[F.col(column).cast(T.ShortType()) for column in columns]   # casting to short 'cause long by default (it's Python)
)

short_df.printSchema()


# %%
short_df.show()


# %%
short_df = short_df.withColumn("overflow", F.col("columnA") * F.col("columnB"))

short_df
# DataFrame[columnA: smallint, columnB: smallint, overflow: smallint]

short_df.show()


# %%
df_numerical = spark.createDataFrame(
    [[2 ** 16, 2 ** 33, 2 ** 65]],
    schema=["sixteen", "thirty-three", "sixty-five"],
)

df_numerical.printSchema()


# %%
df_numerical.show()


# %%
df_short = spark.createDataFrame([[2 ** 15]], schema=["my_read_value"])

df_short = df_short.withColumn(
    "my_short_value", F.col("my_read_value").cast(T.ShortType())
)

df_short.show()


# %%
doubles = spark.createDataFrame([[0.1, 0.2]], ["zero_one", "zero_two"])

doubles.withColumn(
    "zero_three", F.col("zero_one") + F.col("zero_two") - F.col("zero_one")
).show()


# %%
floats = doubles.select([F.col(i).cast(T.FloatType()) for i in doubles.columns])

floats.withColumn(
    "zero_three", F.col("zero_one") + F.col("zero_two") - F.col("zero_one")
).show()


# %%
integer_values = spark.createDataFrame(
    [[0], [1024], [2 ** 17 + 14]], ["timestamp_as_long"]
)

for ts in ["UTC", "America/Toronto", "Europe/Warsaw"]:
    spark.conf.set("spark.sql.session.timeZone", ts)

    ts_values = integer_values.withColumn(
        f"ts_{ts}", F.col("timestamp_as_long").cast(T.TimestampType())
    )

    print(f"==={spark.conf.get('spark.sql.session.timeZone')}===")

    ts_values.show()


# %%
spark.conf.set("spark.sql.session.timeZone", "UTC")

some_timestamps = spark.createDataFrame(
    [["2019-04-01 17:39"], ["2020-07-17 12:45"], ["1994-12-03 00:45"]],
    ["as_string"],
)

some_timestamps = some_timestamps.withColumn(
    "as_timestamp", F.to_timestamp(F.col("as_string"))
)

some_timestamps.show()


# %%
more_timestamps = spark.createDataFrame(
    [["04/01/2019 5:39PM"], ["07/17/2020 12:45PM"], ["12/03/1994 12:45AM"]],
    ["as_string"],
)

more_timestamps = more_timestamps.withColumn(
    "as_timestamp", F.to_timestamp(F.col("as_string"), "M/d/y h:mma")
)

more_timestamps.show()


# %%
this_will_fail_to_parse = more_timestamps.withColumn(
    "as_timestamp", F.to_timestamp(F.col("as_string"))
)

this_will_fail_to_parse.show()  # just nulls


# %%
import datetime as d

some_timestamps = (
    spark.createDataFrame(
        [["2019-04-01 17:39"], ["2020-07-17 12:45"], ["1994-12-03 00:45"]],
        ["as_string"],
    )
    .withColumn("as_timestamp", F.to_timestamp(F.col("as_string")))
    .drop("as_string")
)

some_timestamps = (
    some_timestamps.withColumn(
        "90_days_turnover", F.date_add(F.col("as_timestamp"), 90)
    )
    .withColumn("90_days_turnunder", F.date_sub(F.col("as_timestamp"), 90))
    .withColumn(
        "how_far_ahead",
        F.datediff(F.lit(d.datetime(2020, 1, 1)), F.col("as_timestamp")),
    )
    .withColumn(
        "in_the_future",
        F.when(F.col("how_far_ahead") < 0, True).otherwise(False),
    )
)

some_timestamps.show()


# %%
some_nulls = spark.createDataFrame(
    [[1], [2], [3], [4], [None], [6]], ["values"]
)

some_nulls.groupBy("values").count().show()


# %%
some_nulls.where(F.col("values").isNotNull()).groupBy(
    "values"
).count().show()


# %%
# this_wont_work = spark.createDataFrame([None], "null_column")


# %%
array_df = spark.createDataFrame(
    [[[1, 2, 3]], [[4, 5, 6]], [[7, 8, 9]], [[10, None, 12]]], ["array_of_ints"]
)

array_df.printSchema()


# %%
array_df.show()


# %%
pokedex = spark.read.csv("data/pokedex.dsv", sep="\t").toDF(
    "number", "name", "name2", "type1", "type2"
)

pokedex.show(5)


# %%
pokedex = (
    pokedex.withColumn(
        "type2",
        F.when(F.isnull(F.col("type2")), F.col("type1")).otherwise(
            F.col("type2")
        ),
    )
    .withColumn("type", F.array(F.col("type1"), F.col("type2")))
    .select("number", "name", "type")
)

pokedex.show(5)


# %%
pokedex.withColumn("type", F.array_sort(F.col("type"))).withColumn(
    "type", F.array_distinct(F.col("type"))
).where(
    F.size(F.col("type")) > 1
).groupby(
    "type"
).count().orderBy(
    "count", ascending=False
).show(10)


# %%
# Choice, strong against, weak against
rock_paper_scissor = [
    ["rock", "scissor", "paper"],
    ["paper", "rock", "scissor"],
    ["scissor", "paper", "rock"],
]

rps_df = spark.createDataFrame(
    rock_paper_scissor, ["choice", "strong_against", "weak_against"]
)

rps_df = rps_df.withColumn(
    "result",
    F.create_map(
        F.col("strong_against"),
        F.lit("win"),
        F.col("weak_against"),
        F.lit("lose"),
    ),
).select("choice", "result")

rps_df.printSchema()


# %%
rps_df.select(
    F.col("choice"), F.col("result")["rock"], F.col("result.rock")
).show()


# %%
rps_df.select(F.map_values(F.col("result"))).show()


# %%
pokedex_schema = T.StructType(
    [
        T.StructField("number", T.StringType(), False, None),
        T.StructField("name", T.StringType(), False, None),
        T.StructField("name2", T.StringType(), False, None),
        T.StructField("type1", T.StringType(), False, None),
        T.StructField("type2", T.StringType(), False, None),
    ]
)

pokedex = spark.read.csv(
    "data/pokedex.dsv", sep="\t", schema=pokedex_schema
)

pokedex.printSchema()


# %%
pokedex_struct = pokedex.withColumn(
    "pokemon", F.struct(F.col("name"), F.col("type1"), F.col("type2"))
)

pokedex_struct.printSchema()


# %%
pokedex_struct.show(5, False)


# %%
pokedex_struct.select("name", "pokemon.name").show(5)


# %%
pokedex_struct.select("pokemon.*").printSchema()


# %%
pokedex = spark.read.csv("data/pokedex.dsv", sep="\t").toDF(
    "number", "name", "name2", "type1", "type2"
)

transformations = [
    F.col("name").alias("pokemon_name"),
    F.when(F.col("type1").isin(["Fire", "Water", "Grass"]), True)
        .otherwise(False)
        .alias("starter_type"),
]

transformations


# %%
pokedex.select(transformations).printSchema()


# %%
pokedex.select(transformations).show(5, False)


# %%
row_sample = [
    T.Row(name="Bulbasaur", number=1, type=["Grass", "Poison"]),
    T.Row(name="Charmander", number=4, type=["Fire"]),
    T.Row(name="Squirtle", number=7, type=["Water"]),
]

row_df = spark.createDataFrame(row_sample)

row_df.printSchema()


# %%
row_df.show(3, False)


# %%
row_df.take(3)


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
data = [[str(x // 10)] for x in range(1000)]

data[240] = ["240_0"]
data[543] = ["56.24"]
data[917] = ["I'm an outlier!"]

clean_me = spark.createDataFrame(
    data, T.StructType([T.StructField("values", T.StringType())])
)

clean_me = clean_me.withColumn(
    "values_cleaned", F.col("values").cast(T.IntegerType())
)

clean_me.drop_duplicates().show(10)


# %%
clean_me = clean_me.withColumn(
    "values", F.split(F.col("values"), "_")[0]
)


# %%
cast_df.show(3, False)


# %%
cast_df.fillna(-1).show()


# %%
cast_df.fillna(-1, ["number_with_decimal"]).fillna(-3).show()


# %%
cast_df.fillna({"number_with_decimal": -1, "integer_with_separators": -3}).show()


