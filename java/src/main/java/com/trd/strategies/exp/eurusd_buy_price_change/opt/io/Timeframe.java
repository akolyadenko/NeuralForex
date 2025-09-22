package com.trd.strategies.exp.eurusd_buy_price_change.opt.io;

import com.google.common.primitives.Floats;
import com.trd.util.NiUtil;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    static class Timepoint implements Serializable {
        public float open, high, low, close, volume;

        Timepoint() {}

        Timepoint(Map<String, Object> row) {
            open = (Float)row.get("open");
            high = (Float)row.get("high");
            low = (Float)row.get("low");
            close = (Float)row.get("close");
            volume = (Float)row.get("volume");
        }
    }

    public static class TimeframeCandles {
        public List<Float> open = new ArrayList<>();
        public List<Float> high = new ArrayList<>();
        public List<Float> low = new ArrayList<>();
        public List<Float> close = new ArrayList<>();
        public List<Float> volume = new ArrayList<>();

        TimeframeCandles buildRescaled(float buyPrice, float maxVolume) {
            var rescaled = new TimeframeCandles();
            rescaled.open = rescalePrice(open, buyPrice);
            rescaled.high = rescalePrice(high, buyPrice);
            rescaled.low = rescalePrice(low, buyPrice);
            rescaled.close = rescalePrice(close, buyPrice);
            rescaled.volume = rescaleVolume(volume, maxVolume);
            return rescaled;
        }

        List<Float> rescalePrice(List<Float> l, float buy) {
            return l.stream()
                    .map(v -> (v - buy) * 1000)
                    .collect(Collectors.toList());
        }

        List<Float> rescaleVolume(List<Float> l, float max) {
            if (max == 0) {
                return l;
            }
            var log = (int) Math.log10(max);
            if (log < 1) {
                return l;
            }
            var div = (float)Math.pow(10, log);
            return l.stream()
                    .map(v -> v / div)
                    .collect(Collectors.toList());
        }

        void takeLast(int n) {
            open = NiUtil.takeLast(open, n);
            high = NiUtil.takeLast(high, n);
            close = NiUtil.takeLast(close, n);
            low = NiUtil.takeLast(low, n);
            volume = NiUtil.takeLast(volume, n);
        }

        public static class RangeLowsIndicators {
            public List<Float> lows = new ArrayList<>();
            public List<Float> proximity5pCounts = new ArrayList<>();
            public List<Float> proximity10pCounts = new ArrayList<>();
            public List<Float> proximity20pCounts = new ArrayList<>();
            public List<Float> linearA = new ArrayList<>();
            public List<Float> linearB = new ArrayList<>();
        }

        RangeLowsIndicators createRangeLows() {
            var res = new RangeLowsIndicators();
            int frameSize = 10;
            while (frameSize <= low.size()) {
                var sublist = low
                        .subList(low.size() - frameSize, low.size());
                float rangeLow = sublist
                        .stream()
                        .min(Float::compare)
                        .get();
                float rangeHigh = high
                        .subList(high.size() - frameSize, high.size())
                        .stream()
                        .max(Float::compare)
                        .get();
                res.lows.add(rangeLow);
                var p5Count = 0f;
                var p10Count = 0f;
                var p20Count = 0f;
                for (var val: sublist) {
                    if (rangeLow != rangeHigh
                            && Math.abs(rangeLow - val) / Math.abs(rangeLow - rangeHigh) < 0.05) {
                        p5Count += 1;
                    }
                    if (rangeLow != rangeHigh
                            && Math.abs(rangeLow - val) / Math.abs(rangeLow - rangeHigh) < 0.10) {
                        p10Count += 1;
                    }
                    if (rangeLow != rangeHigh
                            && Math.abs(rangeLow - val) / Math.abs(rangeLow - rangeHigh) < 0.20) {
                        p20Count += 1;
                    }
                }
                res.proximity5pCounts.add(p5Count);
                res.proximity10pCounts.add(p10Count);
                res.proximity20pCounts.add(p20Count);
                var regr = new SimpleRegression();
                for (int i = 0; i < sublist.size(); i ++) {
                    regr.addData(i - sublist.size() + 1, sublist.get(i));
                }
                res.linearA.add(Double.valueOf(regr.getSlope()).floatValue());
                res.linearB.add(Double.valueOf(regr.getIntercept()).floatValue());
                frameSize += 10;
            }
            return res;
        }

        public static class RangeHighsIndicators {
            public List<Float> highs = new ArrayList<>();
            public List<Float> proximity5pCounts = new ArrayList<>();
            public List<Float> proximity10pCounts = new ArrayList<>();
            public List<Float> proximity20pCounts = new ArrayList<>();
            public List<Float> linearA = new ArrayList<>();
            public List<Float> linearB = new ArrayList<>();
        }

        RangeHighsIndicators createRangeHighs() {
            var res = new RangeHighsIndicators();
            int frameSize = 10;
            while (frameSize <= high.size()) {
                var sublist = high
                        .subList(high.size() - frameSize, high.size());
                float rangeHigh = sublist
                        .stream()
                        .max(Float::compare)
                        .get();
                float rangeLow = low
                        .subList(low.size() - frameSize, low.size())
                        .stream()
                        .min(Float::compare)
                        .get();
                res.highs.add(rangeHigh);
                var p5Count = 0f;
                var p10Count = 0f;
                var p20Count = 0f;
                for (var val: sublist) {
                    if (rangeLow != rangeHigh
                            && Math.abs(rangeHigh - val) / Math.abs(rangeLow - rangeHigh) < 0.05) {
                        p5Count += 1;
                    }
                    if (rangeLow != rangeHigh
                            && Math.abs(rangeHigh - val) / Math.abs(rangeLow - rangeHigh) < 0.10) {
                        p10Count += 1;
                    }
                    if (rangeLow != rangeHigh
                            && Math.abs(rangeHigh - val) / Math.abs(rangeLow - rangeHigh) < 0.20) {
                        p20Count += 1;
                    }
                }
                res.proximity5pCounts.add(p5Count);
                res.proximity10pCounts.add(p10Count);
                res.proximity20pCounts.add(p20Count);
                var regr = new SimpleRegression();
                for (int i = 0; i < sublist.size(); i ++) {
                    regr.addData(i - sublist.size() + 1, sublist.get(i));
                }
                res.linearA.add(Double.valueOf(regr.getSlope()).floatValue());
                res.linearB.add(Double.valueOf(regr.getIntercept()).floatValue());
                frameSize += 10;
            }
            return res;
        }
    }
}
