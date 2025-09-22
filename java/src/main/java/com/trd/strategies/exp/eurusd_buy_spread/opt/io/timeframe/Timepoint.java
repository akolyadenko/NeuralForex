package com.trd.strategies.exp.eurusd_buy_spread.opt.io.timeframe;

import java.io.Serializable;
import java.util.Map;

public class Timepoint implements Serializable {
    public float open, high, low, close, volume;

    public Timepoint() {
    }

    public Timepoint(Map<String, Object> row) {
        open = (Float) row.get("open");
        high = (Float) row.get("high");
        low = (Float) row.get("low");
        close = (Float) row.get("close");
        volume = (Float) row.get("volume");
    }
}
