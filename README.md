Project implements end-to-end Forex trading workflow with sofisticated deep learning models and bidding strategies search.

## Historical data

[OandaLoadHistoricalData.java](https://github.com/akolyadenko/NeuralForex/blob/main/java/src/main/java/com/trd/broker/OandaLoadHistoricalData.java) imports 3 years of historical EUR/USD price data from Oanda and stores it in PostrgeSQL database.  

## Feature engineering

Various features are generated from historical data per each strategy and produced as TFRecord file. Example [eurusd_buy_spread/opt/io/CreateTfRecordFile.java](https://github.com/akolyadenko/NeuralForex/blob/main/java/src/main/java/com/trd/strategies/prod/eurusd_buy_spread/opt/io/CreateTfRecordFile.java)

## Neural models for price prediction

Neural Tensorflow models predict probability of price pip change. They generate histogram of probabilities for incremental price change.

Examples of models:

- simple DNN model: [eurusd_buy_spread/train_model_dense.py](https://github.com/akolyadenko/NeuralForex/blob/main/py/strategies/prod/eurusd_buy_spread/train_model_dense.py)
- convolution tree network, which convolutes multiple timeframes: [eurusd_buy_spread/train_model_tree.py](https://github.com/akolyadenko/NeuralForex/blob/main/py/strategies/prod/eurusd_buy_spread/train_model_tree.py)
- recurrent network, where each recurrent node is a block of complex subnetwork inside: [eurusd_buy_spread/train_model_recb.py](https://github.com/akolyadenko/NeuralForex/blob/main/py/strategies/prod/eurusd_buy_spread/train_model_recb.py)
- transformer based model: [eurusd_buy_spread/train_model_transformer.py](https://github.com/akolyadenko/NeuralForex/blob/main/py/strategies/prod/eurusd_buy_spread/train_model_transformer.py)

