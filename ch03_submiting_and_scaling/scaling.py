# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# Before
# book = (spark
#            .read.text("../data/pride-and-prejudice.txt")

# After
# books = (spark
#            .read.text("../data/many-books/*.txt")
