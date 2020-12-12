# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# %%
from operator import add

exo_rdd = spark.sparkContext.parallelize(list(range(100)))

exo_rdd.map(lambda _: 1).reduce(add)

# %%
sc = spark.sparkContext

a_rdd = sc.parallelize([0, 1, None, [], 0.0])
a_rdd.filter(lambda x: x).collect()

# %%
from fractions import Fraction

Fraction(1, 4) + Fraction(1, 4)

# %%
from typing import Tuple, Optional

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

Frac = Tuple[int, int]
SparkFrac = T.ArrayType(T.LongType())

@F.udf(SparkFrac)
def add_fractions(frac1: Frac, frac2: Frac) -> Frac:
    """Add two fractions represented as a 2-tuple of integers."""
    fraction = Fraction(*frac1) + Fraction(*frac2)
    return fraction.numerator, fraction.denominator

assert add_fractions.func((1, 4), (1, 4)) == (1, 2)

# %%
fractions = [[x, y] for x in range(10) for y in range(1, 10)]

frac_df = spark.createDataFrame(fractions, ["numerator", "denominator"])

frac_df = frac_df.select(
    F.array(F.col("numerator"), F.col("denominator")).alias("fraction"),
)

frac_df = frac_df.withColumn(
    "addition", add_fractions(F.col("fraction"), F.col("fraction"))
)

frac_df.select("fraction", "addition").distinct().show(20, False)

# %%
def py_reduce_fraction(frac: Frac) -> Optional[Frac]:
    """Reduce a fraction represented as a 2-tuple of integers."""
    num, denom = frac

    if num > pow(2, 63) - 1 or denom < -pow(2, 63):
        return None

    if denom:
        answer = Fraction(num, denom)
        return answer.numerator, answer.denominator

    return None


assert py_reduce_fraction((3, 6)) == (1, 2)
assert py_reduce_fraction((1, 0)) is None
assert py_reduce_fraction((pow(2, 63), 0)) is None
assert py_reduce_fraction((0, -pow(2, 64))) is None

# %%

