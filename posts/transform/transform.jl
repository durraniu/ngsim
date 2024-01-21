# Load parquet file
# ] add Parquet2
using Parquet2, DataFrames

path_to_first_period_file = "posts/transform/data/i80_period1.parquet"
data_jl = DataFrame(load(path_to_first_period_file))