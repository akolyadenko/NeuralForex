select
    example.epoch_min,
    open,
    close,
    high,
    low,
    predictions
from example
         join example_prediction on example_prediction.epoch_min = example.epoch_min
         join timeframe on timeframe.epoch_min = example.epoch_min
where example_prediction.split = 'test'
  and type = 'm1'
order by epoch_min;
