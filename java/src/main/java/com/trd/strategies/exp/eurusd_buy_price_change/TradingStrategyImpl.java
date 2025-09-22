package com.trd.strategies.exp.eurusd_buy_price_change;

import com.trd.broker.OandaClient;
import com.trd.strategies.TradingStrategy;
import com.trd.trader.TickData;
import com.trd.util.NiUtil;

public class TradingStrategyImpl extends TradingStrategy {
    public static final long TRADE_UNITS = 3 * 1000 * 1000;

    @Override
    public void processTick(TickData tickData) throws Exception {
        var activeTrade = tickData.positions.positions.size() > 0;
        if (activeTrade && tickData.predictions.pricePredictionsDelta.get(5) < -0.5) {
            System.out.println("TraderMain.main close positions: " + OandaClient.closeAllPositions());
        }
        if (!activeTrade && tickData.predictions.pricePredictionsDelta.get(10) > 0.5
                && Math.abs(tickData.timeframes.m1.lastCandle.bid.c
                    - tickData.timeframes.m1.lastCandle.ask.c) < 0.00025) {
            var lastCandlePrice = tickData.timeframes.m1.lastCandle.bid.c;
            System.out.println("TraderMain.main last candle close: " + lastCandlePrice);
            var stopLoss = lastCandlePrice - 0.0019f;
            System.out.println("TraderMain.main stop loss: " + stopLoss);
            System.out.println("TraderMain.main open position: " +
                    NiUtil.GSON.toJson(
                            OandaClient.submitTrade(TRADE_UNITS, stopLoss)));
        }
    }
}
