select
    tp.time,
    predictions[106] - predictions[6] b5,
    predictions[111] - predictions[11] b10,
    tf.close close_price,
    tr.buy_price,
    tr.profit,
    tf.close - tr.buy_price - 0.0004 current_profit,
    -- m1close[array_length(m1close, 1) - 5:],
    features.data
from example_prediction pr
join example_prediction_features features using (epoch_min)
join timepoint tp using (epoch_min)
join timeframe tf on tf.epoch_min = tp.epoch_min
    and tf.type = 'm1'
left join walk_trade tr on
    tr.enter_epoch_min <= tp.epoch_min
    and tr.exit_epoch_min >= tp.epoch_min
where tp.time = '2022-06-20 02:31:00+00'
  and features.type = 'm5close'
order by tp.epoch_min desc;
