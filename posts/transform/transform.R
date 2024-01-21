# Create one file
library(arrow)
library(dplyr)

# path_to_partitioned_first_period_file <- "posts/import/data/I80/period=first/part-0.parquet"
# 
# open_dataset(path_to_partitioned_first_period_file) |> 
#   dplyr::collect() |> 
#   arrow::write_parquet(sink = "posts/transform/data/i80_period1.parquet")





# Load parquet file 
path_to_first_period_file <- "posts/transform/data/i80_period1.parquet"
data_R <- arrow::read_parquet(file = path_to_first_period_file)

data_R



# Clean dataframe names
library(janitor)
data_R <- janitor::clean_names(data_R) 




# How to create a time column?
library(lubridate)

data_R <- data_R |>
  dplyr::mutate(actual_time = lubridate::as_datetime(global_time / 1000, 
    origin = "1970-01-01",
    tz = "America/Los_Angeles"
  ))


data_R <- data_R |> 
  group_by(vehicle_id) |> 
  mutate(time = (0:(n()-1))/10) |> 
  ungroup()


# How to create variables for the preceding vehicle?

## First: Sort by Vehicle ID and Time
data_R <- data_R |> 
  arrange(vehicle_id, frame_id)

## Then create new cols
data_R <- data_R |> 
  dplyr::group_by(frame_id) |>  # grouping by frame id
  dplyr::mutate(preceding_local_y = local_y[match(preceding, vehicle_id)],
                preceding_local_x = local_x[match(preceding, vehicle_id)],
                preceding_length = v_length[match(preceding, vehicle_id)],
                preceding_width = v_width[match(preceding, vehicle_id)],
                preceding_class = v_class[match(preceding, vehicle_id)],
                preceding_vel = v_vel[match(preceding, vehicle_id)],
                preceding_acc = v_acc[match(preceding, vehicle_id)]) |> 
  dplyr::ungroup()





# How to remove undesired columns?
data_R <- data_R |>
  select(-c(total_frames, starts_with("global"), following, 
            o_zone, d_zone, int_id, section_id, direction, 
            movement), global_time) 


# How to transform multiple columns?
## metric units
data_R <- data_R |> 
  mutate(across(
    .cols = c(starts_with("local"), starts_with("v_"), space_headway, starts_with("preceding"), -preceding, -preceding_class, -v_class),
    .fns = ~ round(.x * .3048, 2)
  ))

## factor type
data_R <- data_R |> 
  mutate(across(
  .cols = c(vehicle_id, v_class, lane_id, preceding, preceding_class),
  .fns = ~ as.factor(.x)
))






# Visualization with one vehicle
data_R_veh <- data_R |> 
  dplyr::filter(vehicle_id == "2")

library(ggplot2)
ggplot(data = data_R_veh,
       mapping = aes(x = time, y = v_vel)) +
  geom_path() +
  labs(x = "Time (s)", y = "Velocity (m/s)",
       title = "Velocity of vehicle # 2000") +
  theme_minimal()



ggplot(data = data_R_veh |> filter(time == 10.0)) +
  geom_rect(aes(fill = "Preceding Veh", 
                xmin = preceding_local_y - preceding_length,
                xmax = preceding_local_y,
                ymin = preceding_local_x - (preceding_width/2),
                ymax = preceding_local_x + (preceding_width/2))) +
  geom_rect(aes(fill = "Subject Veh",
                xmin = local_y - v_length,
                xmax = local_y,
                ymin = local_x - (v_width/2),
                ymax = local_x + (v_width/2))) +
  geom_hline(yintercept = 3.6, linetype = "longdash") +
  coord_fixed(ratio=1) +
  labs(fill = NULL) +
  theme_minimal()



library(gganimate)
my.animation <- ggplot(data = data_R_veh) +
  geom_rect(aes(fill = "Preceding Veh", 
                xmin = preceding_local_y - preceding_length,
                xmax = preceding_local_y,
                ymin = preceding_local_x - (preceding_width/2),
                ymax = preceding_local_x + (preceding_width/2))) +
  geom_rect(aes(fill = "Subject Veh",
                xmin = local_y - v_length,
                xmax = local_y,
                ymin = local_x - (v_width/2),
                ymax = local_x + (v_width/2))) +
  geom_hline(yintercept = 3.6, linetype = "longdash") +
  coord_fixed(ratio=3.5) +
  labs(fill = NULL) +
  theme_minimal() + 
  theme(legend.position = "none") +
  transition_reveal(time)

animate(my.animation, height = 3,
        width = 8, units = "in", res = 300)


anim_save(filename = "posts/transform/car_following.gif")
