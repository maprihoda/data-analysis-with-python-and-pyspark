# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

shows = spark.read.json("../data/shows-silicon-valley.json")

# %%
print(shows.count())

assert shows.count() == 1

# %%
three_shows = spark.read.json(
    "../data/shows-*.json", multiLine=True
)

# %%
print(three_shows.count())

assert three_shows.count() == 3

three_shows.printSchema()

three_shows.show()

# %%
shows.printSchema()

print(shows.columns)

array_subset = shows.select("name", "genres")

array_subset.show()

# %%
import pyspark.sql.functions as F

array_subset = array_subset.select(
    "name",
    array_subset.genres[0].alias("dot_and_index"),
    F.col("genres")[0].alias("col_and_index"),
    array_subset.genres.getItem(0).alias("dot_and_method"),
    F.col("genres").getItem(0).alias("col_and_method"),
)

array_subset.show()

# %%
array_subset.select(
    "name",
    F.lit("Comedy").alias("one"),
    F.lit("Horror").alias("two"),
    F.lit("Drama").alias("three"),
    F.col("dot_and_index"),
).show()

# %%
array_subset_repeated = array_subset.select(
    "name",
    F.lit("Comedy").alias("one"),
    F.lit("Horror").alias("two"),
    F.lit("Drama").alias("three"),
    F.col("dot_and_index"),
).select(
    "name",
    F.array("one", "two", "three").alias("Some_Genres"),
    F.array_repeat("dot_and_index", 5).alias("Repeated_Genres"),
)

array_subset_repeated.show(truncate=False)

# %%

array_subset_repeated.select(
    "name", F.size("Some_Genres"), F.size("Repeated_Genres")
).show()


array_subset_repeated.select(
    "name",
    F.array_distinct("Some_Genres"),
    F.array_distinct("Repeated_Genres"),
).show(1, False)


array_subset_repeated = array_subset_repeated.select(
    "name",
    F.array_intersect("Some_Genres", "Repeated_Genres").alias(
        "Genres"
    ),
)

array_subset_repeated.show()

# %%
array_subset_repeated.select(
    "Genres", F.array_position("Genres", "Comedy")  # starts with 1
).show()

# %%
columns = ["name", "language", "type"]

shows_map = shows.select(
    *[F.lit(column) for column in columns],
    F.array(*columns).alias("values")
)

shows_map.show(1, False)

# %%
shows_map = shows_map.select(F.array(*columns).alias("keys"), "values")

shows_map.show(1, False)

# %%
shows_map = shows_map.select(
    F.map_from_arrays("keys", "values").alias("mapped")
)

shows_map.printSchema()

shows_map.show(1, False)

# %%
shows_map.select(
    F.col("mapped.name"),
    F.col("mapped")["name"],
    shows_map.mapped["name"],
).show()


# %%
shows.select("schedule").printSchema()

# %%
shows.select(F.col("_embedded")).printSchema()

shows_clean = shows.withColumn("episodes", F.col("_embedded.episodes")).drop(
    "_embedded"
)

shows_clean.printSchema()

# %%
episodes_name = shows_clean.select(F.col("episodes.name"))
episodes_name.printSchema()
episodes_name.show(1, truncate=False)

# %%
episodes_name.select(F.explode("name").alias("name")).show(3, False)

# %%
episodes = shows.select(
    "id",
    F.explode("_embedded.episodes").alias("episodes")  # sans the keys, just values
)

episodes.show(5)

episodes.count()

# %%
episode_name_id = shows.select(
    F.map_from_arrays(
        F.col("_embedded.episodes.id"), F.col("_embedded.episodes.name")
    ).alias("name_id_mapping")
)

episode_name_id.show(1)

# %%
episode_name_id = episode_name_id.select(
    F.posexplode("name_id_mapping").alias("position", "id", "name")
)

episode_name_id.show(5)

# %%
collected = episodes.groupBy("id").agg(F.collect_list("episodes").alias("episodes"))

collected.show()

print(collected.count())

collected.printSchema()

# %%
struct_ex = shows.select(
    F.struct(
        F.col("status"), F.col("weight"), F.lit(True).alias("has_watched")
    ).alias("info")
)

struct_ex.show(1, False)

struct_ex.printSchema()

# struct, when displayed, only shows the values, not the keys
