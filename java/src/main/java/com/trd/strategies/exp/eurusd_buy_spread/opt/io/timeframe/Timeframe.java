package com.trd.strategies.exp.eurusd_buy_spread.opt.io.timeframe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Timeframe {
    public List<Timepoint> timepoints = new ArrayList<>();
    public TimeframeCandles candles = new TimeframeCandles();
    public TimeframeCandles rescaledCandles = new TimeframeCandles();
    public TimeframeCandles.RangeLowsIndicators rangeLows;
    public TimeframeCandles.RangeHighsIndicators rangeHighs;

    public void initVectors() {
        for (var tpnt : timepoints) {
            candles.open.add(tpnt.open);
            candles.high.add(tpnt.high);
            candles.low.add(tpnt.low);
            candles.close.add(tpnt.close);
            candles.volume.add(tpnt.volume);
        }
    }

    public void buildIndicators() {
        this.rangeLows = rescaledCandles.createRangeLows();
        this.rangeHighs = rescaledCandles.createRangeHighs();
    }

    public float minPrice() {
        var prices = new ArrayList<Float>();
        prices.addAll(candles.open);
        prices.addAll(candles.high);
        prices.addAll(candles.low);
        prices.addAll(candles.close);
        return Collections.min(prices);
    }

    public float maxVolume() {
        return Collections.max(candles.volume);
    }

    public void buildRescaled(float buyPrice, float maxVolume) {
        rescaledCandles = candles.buildRescaled(buyPrice, maxVolume);
    }

}
