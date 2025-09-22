select
    time,
    price_increase_predictions[6]-price_decrease_predictions[6] delta_5,
    price_increase_predictions[11]-price_decrease_predictions[11] delta_10,
    m1time[array_length(m1time, 1) - 20:],
    m5time[array_length(m5time, 1) - 20:]
    -- data
from trader_tick_data_expanded
join trader_tick_data_expanded_features using (time)
where time = '2022-06-20 02:31:01.199+0000'
    and type = 'm5close_rescaled'
order by time desc;