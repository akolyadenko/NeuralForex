package com.trd.strategies;

import com.trd.trader.TickData;

public abstract class TradingStrategy {
    public abstract void processTick(TickData tickDtata) throws Exception;
}
