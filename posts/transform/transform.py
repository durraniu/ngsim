# >>> import polars as pl
# C:\Users\umair\anaconda3\envs\homl3\lib\site-packages\polars\_cpu_check.py:239: RuntimeWarning: Missing required CPU features.

# The following required CPU features were not detected:
#     avx2, fma, bmi1, bmi2, lzcnt
# Continuing to use this version of Polars on this processor will likely result in a crash.
# Install the `polars-lts-cpu` package instead of `polars` to run Polars with better compatibility.

# Load parquet file
# pip install polars-lts-cpu
import polars as pl

path_to_first_period_file = "posts/transform/data/i80_period1.parquet"
data_py = pl.read_parquet(path_to_first_period_file)

data_py


# Clean dataframe names
# pip install pyjanitor
# pip install pyarrow
# pip install pandas
import pandas as pd
import pyarrow
import janitor
data_py = data_py.to_pandas()
data_py = data_py.clean_names()

# How to create a time column?
data_py['actual_time'] = pd.to_datetime(data_py['global_time'] / 1000, 
                                    unit='s', origin='1970-01-01', utc=True)
data_py['actual_time'] = data_py['actual_time'].dt.tz_convert('America/Los_Angeles')

## First: Sort by Vehicle ID and Time
data_py = data_py.sort_values(by = ["vehicle_id", "frame_id"])

def calculate_time_elapsed(group_df):
    num_rows = len(group_df)
    group_df['time'] = [i / 10.0 for i in range(num_rows)]
    return group_df

# Add the time elapsed column to the DataFrame within each group
data_py = data_py.groupby('vehicle_id', group_keys=False).apply(calculate_time_elapsed)




# How to create variables for the preceding vehicle?
## Then create new cols
data_py = data_py.merge(
  data_py.loc[:, ['frame_id', 'vehicle_id', 'local_x', 'local_y', 'v_length',
            'v_width', 'v_class', 'v_vel', 'v_acc']] , 
              left_on = ['frame_id', 'preceding'], 
              right_on = ['frame_id', 'vehicle_id'], how = 'left', 
              suffixes=['', '_preceding']
              )
data_py = data_py.drop(['vehicle_id_preceding'], axis = 'columns')



data_py = pl.from_pandas(data_py)

data_py.columns

data_py = data_py.rename({
    "local_y_preceding":"preceding_local_y", 
    "v_length_preceding":"preceding_v_length", 
    "v_width_preceding":"preceding_v_width", 
    "v_class_preceding":"preceding_v_class", 
    "v_vel_preceding":"preceding_v_vel", 
    "v_acc_preceding":"preceding_v_acc"
    })

data_py.columns



# How to remove undesired columns?
data_py = data_py.drop(["total_frames", "global_x", "global_y", "following", 
              "o_zone", "d_zone", "int_id", "section_id", "direction", 
             "movement"])



# How to transform multiple columns?
## convert to metric
cols_to_convert_to_metric = ['local_x', 'local_y', 'v_length', 'v_width', 
        'v_vel', 'v_acc', 'space_headway', 'preceding_local_y',
        'preceding_v_length', 'preceding_v_width', 'preceding_v_vel',
       'preceding_v_acc']

data_py = data_py.with_columns((pl.col(cols_to_convert_to_metric) * .3048).round(2))

## change the data type to categorical
cols_to_convert_to_categorical = ['vehicle_id', 'v_class', 'lane_id', 
                             'preceding', 'preceding_v_class']
data_py = data_py.with_columns(pl.col(cols_to_convert_to_categorical).cast(pl.String).cast(pl.Categorical))



data_py.shape



from lets_plot import *
LetsPlot.setup_html()

data_py_veh = data_py.filter(pl.col('vehicle_id') == "2")
(
ggplot(data = data_py_veh) +\
  geom_path(mapping = aes(x = 'time', y = 'v_vel', color = "Subject vehicle")) +\
  geom_path(mapping = aes(x = 'time', y =' preceding_vel', color = "Preceding vehicle")) +\
  labs(x = "Time (s)", y = "Velocity (m/s)",
       title = "Velocity of vehicle # 2 and its preceding vehicle") +\
  theme_minimal()
)