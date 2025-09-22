Project implements end-to-end Forex trading workflow with sofisticated deep learning models and bidding strategies search.

## Historical data
[OandaLoadHistoricalData.java](https://github.com/akolyadenko/NeuralForex/blob/main/java/src/main/java/com/trd/broker/OandaLoadHistoricalData.java) imports 3 years of historical EUR/USD price data from Oanda and stores it in PostrgeSQL database.  

## Feature engineering

Various features are generated from historical data per each strategy. Example [CreateTfRecordFile.java](https://github.com/akolyadenko/NeuralForex/blob/main/java/src/main/java/com/trd/strategies/prod/eurusd_buy_spread/opt/io/CreateTfRecordFile.java)
