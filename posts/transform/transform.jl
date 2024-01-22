# Load parquet file
# ] add Parquet
using DataFrames, Parquet
path_to_first_period_file = "posts/transform/data/i80_period1.parquet"
data_jl = DataFrame(read_parquet(path_to_first_period_file))