# Load parquet file
# ] add Parquet
using DataFrames
using Parquet2: Dataset
path_to_first_period_file = "data/i80_period1.parquet"
data_jl = DataFrame(Dataset(path_to_first_period_file));


using TidierData
# @glimpse(data_jl)



data_jl = @clean_names(data_jl)
first(data_jl)


# https://github.com/TidierOrg/TidierData.jl/issues/1#issuecomment-1656262978
data_jl = @select(data_jl, Not(:o_zone, :d_zone, :int_id, :section_id, :direction, :movement))
count(x -> ismissing(x), data_jl.frame_id)
for col in names(data_jl)
    data_jl[!, col] .= coalesce.(data_jl[!, col], 0)
end
# @glimpse(data_jl)


using Dates

data_jl.actual_time = DateTime.(Dates.unix2datetime.(data_jl.global_time ./ 1000))
data_jl.actual_time = data_jl.actual_time .- Hour(7)


data_jl = @arrange(data_jl, vehicle_id, frame_id)
first(data_jl, 5)


data_jl = @chain data_jl begin
    @group_by(vehicle_id)
    @mutate(time = (0:(first(n()) - 1))/10)
    @ungroup()
end




data_jl = leftjoin(
           data_jl,
           data_jl[:, [:frame_id, :vehicle_id, :local_y, :v_length, :v_width, :v_class, :v_vel, :v_acc]],
           on = [:frame_id => :frame_id, :preceding => :vehicle_id],
           makeunique = true, 
           renamecols = "" => "_preceding"
       )

names(data_jl)

data_jl = @arrange(data_jl, vehicle_id, frame_id)
first(data_jl, 5)

data_jl = @chain data_jl begin
    @rename(preceding_local_y = local_y_preceding,
            preceding_length =  v_length_preceding,
            preceding_width = v_width_preceding,
            preceding_class = v_class_preceding,
            preceding_vel = v_vel_preceding,
            preceding_acc = v_acc_preceding)
end



data_jl = @select(data_jl, Not(:total_frames, starts_with("global"), :following))



# data_jl = @chain data_jl begin
#     @mutate(across((
#         starts_with("local"),
#         v_length, v_width, v_vel, v_acc,
#         space_headway,
#         preceding_local_y, preceding_length, preceding_width,
#         preceding_vel, preceding_acc
#         ), (x -> round.(x * 0.3048, digits=2))))
# end

function convert_and_round(x)
    return round(x * 0.3048, digits=2)
end

cols_to_mutate = ["local_x", "local_y", "v_length", "v_width", 
                 "v_vel", "v_acc", "space_headway",
                 "preceding_local_y", "preceding_length", "preceding_width",
                 "preceding_vel", "preceding_acc"]

for col in cols_to_mutate
    data_jl[!, col] .= convert_and_round.(data_jl[!, col])
end



data_jl[!,:vehicle_id] = string.(data_jl[!,:vehicle_id])
data_jl[!,:v_class] = string.(data_jl[!,:v_class])
data_jl[!,:lane_id] = string.(data_jl[!,:lane_id])
data_jl[!,:preceding] = string.(data_jl[!,:preceding])
data_jl[!,:preceding_class] = string.(data_jl[!,:preceding_class])



data_jl_veh = @filter(data_jl, vehicle_id == "2")



using TidierPlots
ggplot(data = data_jl_veh) +
  geom_path(aes(x = "time", y = "v_vel"), color = "blue") +
  geom_path(aes(x = "time", y = "preceding_vel"), color = "orange") +
  labs(x = "Time (s)", y = "Velocity (m/s)",
       title = "Velocity of vehicle # 2 and its preceding vehicle") +
  theme_minimal()