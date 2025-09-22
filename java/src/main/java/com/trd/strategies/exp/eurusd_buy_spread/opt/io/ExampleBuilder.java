package com.trd.strategies.exp.eurusd_buy_spread.opt.io;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import com.sun.jdi.LongValue;
import com.trd.db.Db;
import com.trd.strategies.exp.eurusd_buy_spread.opt.io.timeframe.Timeframe;
import com.trd.strategies.exp.eurusd_buy_spread.opt.io.timeframe.TimeframeCandles;
import com.trd.strategies.exp.eurusd_buy_spread.opt.io.timeframe.Timeframes;
import com.trd.strategies.exp.eurusd_buy_spread.opt.io.timeframe.Timepoint;
import com.trd.util.NiStats;
import org.apache.commons.lang3.SerializationUtils;
import org.jdbi.v3.core.Handle;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExampleBuilder {
    public static int NEXT_M1_TMF_SIZE = 240;
    public static float BUY_SELL_SPREAD = 0.0002f;
    public static float VOLUME_NORM_MULTIPLIER = 1f;
    public static float PROFIT_HISTOGRAM_STEP = 0.0001f;

    public static boolean buildExample(
            long epochMin, long year, long month, long day, long hour, long minute,
            List<Timepoint> askPrecedingTimepoints, List<Timepoint> bidPrecedingTimepoints,
            List<Timepoint> askNextTimepoints, List<Timepoint> bidNextTimepoints,
            Features.Builder features) {
        features.putFeature("epoch_min", createFeature(epochMin));
        features.putFeature("year", createFeature(year));
        features.putFeature("month", createFeature(month));
        features.putFeature("day", createFeature(day));
        features.putFeature("day_of_week", createFeature((epochMin / (60 * 24)) % 7));
        features.putFeature("hour", createFeature(hour));
        features.putFeature("minute", createFeature(minute));
        try (var h = Db.jdbi().open()) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.example_requested");
            var m1tfm = createPrecedingTimeframes(h, epochMin, askPrecedingTimepoints, bidPrecedingTimepoints);
            var nextm1tfm = createNextTimeframes(h, epochMin, askNextTimepoints, bidNextTimepoints);
            if (m1tfm == null || nextm1tfm == null) {
                NiStats.counters.incrementAndGet("CreateTfRecordFile.example_skipped");
                return false;
            }
            var m5tfm = aggregateTimeframes(m1tfm, 5);
            var m15tfm = aggregateTimeframes(m1tfm, 15);
            var m30tfm = aggregateTimeframes(m1tfm, 30);
            var currentAsk = m1tfm.ask.timepoints.get(m1tfm.ask.timepoints.size() - 1).close;
            var currentBid = m1tfm.bid.timepoints.get(m1tfm.bid.timepoints.size() - 1).close;
            features.putFeature("current_bid_rescaled", createFeature(Lists.newArrayList(
                    (currentBid - currentAsk) * 1000)));
            var maxVolume = m1tfm.ask.maxVolume();
            for (var tfms : Lists.newArrayList(m1tfm, m5tfm, m15tfm, m30tfm)) {
                tfms.ask.candles.takeLast(60);
                tfms.ask.buildRescaled(currentAsk, maxVolume);
                tfms.ask.buildIndicators();
                tfms.bid.candles.takeLast(60);
                tfms.bid.buildRescaled(currentAsk, maxVolume);
                tfms.bid.buildIndicators();
            }
            NiStats.counters.incrementAndGet("CreateTfRecordFile.example_created");
            addTimeframesFeatures("m1", m1tfm, currentAsk, features);
            addTimeframesFeatures("m5", m5tfm, currentAsk, features);
            addTimeframesFeatures("m15", m15tfm, currentAsk, features);
            addTimeframesFeatures("m30", m30tfm, currentAsk, features);
            // addTimeframeFeatures("nextm1", nextm1tfm, base, features);
            for (var mins : Lists.newArrayList(5, 15, 30, 60, 120, 240)) {
                if (!addPriceIncreaseHistogram(nextm1tfm, features, mins)) {
                    return false;
                }
                if (!addPriceDecreaseHistogram(nextm1tfm, features, mins)) {
                    return false;
                }
                // if (!addPriceChangeFeatures(nextm1tfm, features, mins)) {
                //    return false;
                // }
            }
            addHighLow(nextm1tfm, currentAsk, features);
            return true;
        }
    }

    public static boolean addPriceChangeFeatures(Timeframe nextm1tfm, Features.Builder features, int mins) {
        Preconditions.checkArgument(nextm1tfm.timepoints.size() >= mins);
        var timepoints = nextm1tfm.timepoints.subList(0, mins);
        float buyPrice = timepoints.get(0).open + BUY_SELL_SPREAD;
        var lows = timepoints
                .subList(1, timepoints.size())
                .stream()
                .map(pnt -> pnt.low)
                .collect(Collectors.toList());
        float maxLow = (lows.stream().max(Floats::compare).get() - buyPrice);
        float minLow = (lows.stream().min(Floats::compare).get() - buyPrice);
        float avgLow = 0;
        for (var l : lows) {
            avgLow += (l - buyPrice);
        }
        avgLow /= timepoints.size() - 1;
        if (maxLow > 0) {
            NiStats.counters.incrementAndGet("addPriceChangeFeatures.maxLowPositive");
        }
        if (maxLow < 0) {
            NiStats.counters.incrementAndGet("addPriceChangeFeatures.maxLowNegative");
        }
        features.putFeature("price_change_max_" + mins, createFeature(Lists.newArrayList(maxLow * 1000)));
        features.putFeature("price_change_min_" + mins, createFeature(Lists.newArrayList(minLow * 1000)));
        features.putFeature("price_change_avg_" + mins, createFeature(Lists.newArrayList(avgLow * 1000)));
        return true;
    }

    public static boolean addPriceIncreaseHistogram(Timeframes nextm1tfm, Features.Builder features, int mins) {
        Preconditions.checkArgument(nextm1tfm.ask.timepoints.size() >= mins
                && nextm1tfm.bid.timepoints.size() >= mins);
        var bidTimepoints = nextm1tfm.bid.timepoints.subList(0, mins);
        float buyPrice = nextm1tfm.ask.timepoints.get(0).open;
        float maxHigh = bidTimepoints
                .subList(1, bidTimepoints.size())
                .stream()
                .map(pnt -> pnt.high)
                .max(Floats::compare)
                .get();
        var histogram = new ArrayList<Float>();
        for (int histIdx = 0; histIdx < 100; histIdx++) {
            float profitStop = buyPrice + histIdx * PROFIT_HISTOGRAM_STEP;
            if (profitStop <= maxHigh) {
                histogram.add(1f);
            } else {
                histogram.add(0f);
            }
        }
        features.putFeature("price_increase_histogram_" + mins, createFeature(histogram));
        return true;
    }

    public static boolean addPriceDecreaseHistogram(Timeframes nextm1tfm, Features.Builder features, int mins) {
        Preconditions.checkArgument(nextm1tfm.ask.timepoints.size() >= mins
                && nextm1tfm.bid.timepoints.size() >= mins);
        var askTimepoints = nextm1tfm.ask.timepoints.subList(0, mins);
        float sellPrice = nextm1tfm.bid.timepoints.get(0).open;
        float minLow = askTimepoints
                .subList(1, askTimepoints.size())
                .stream()
                .map(pnt -> pnt.low)
                .min(Floats::compare)
                .get();
        var histogram = new ArrayList<Float>();
        for (int histIdx = 0; histIdx < 100; histIdx++) {
            float profitStop = sellPrice - histIdx * PROFIT_HISTOGRAM_STEP;
            if (profitStop >= minLow) {
                histogram.add(1f);
            } else {
                histogram.add(0f);
            }
        }
        features.putFeature("price_decrease_histogram_" + mins, createFeature(histogram));
        return true;
    }

    public static boolean addHighLow(Timeframes nextm1tfm, float base, Features.Builder features) {
        var highs = new ArrayList<Float>();
        var lows = new ArrayList<Float>();
        for (int mins = 5; mins <= 120; mins += 5) {
            var timepoints = nextm1tfm.bid.timepoints.subList(0, mins);
            highs.add(timepoints.stream()
                    .map(t -> (t.high - base) * 100).max((f1, f2) -> Floats.compare(f1, f2)).get());
            lows.add(timepoints.stream()
                    .map(t -> (t.low - base) * 100).min((f1, f2) -> Floats.compare(f1, f2)).get());
        }
        features.putFeature("next_tfm_highs", createFeature(highs));
        features.putFeature("next_tfm_lows", createFeature(lows));
        return true;
    }

    public static void addTimeframesFeatures(String type, Timeframes tfm, float base, Features.Builder features) {
        addTimeframeFeatures(type + "ask", tfm.ask, base, features);
        addTimeframeFeatures(type + "bid", tfm.bid, base, features);
    }

    public static void addTimeframeFeatures(String type, Timeframe tfm, float base, Features.Builder features) {
        // addTimeframeFeatures(type, "", tfm.candles, features);
        addRescaledCandleFeatures(type, "_rescaled", tfm.rescaledCandles, base, features);
        addRescaledIndicatorFeatues(type, "_rescaled", tfm, features);

        // addTimeframeHistogramFeatures(type, "_rescaled", tfm.rescaledValues, base, features);
    }

    public static void addRescaledCandleFeatures(String type, String suffix, TimeframeCandles values, float base,
                                                 Features.Builder features) {
        addRescaledFloatFeaturesList(type + "open" + suffix, values.open, features);
        addRescaledFloatFeaturesList(type + "high" + suffix, values.high, features);
        addRescaledFloatFeaturesList(type + "low" + suffix, values.low, features);
        addRescaledFloatFeaturesList(type + "close" + suffix, values.close, features);

        features.putFeature(type + "volume" + suffix, createFeature(values.volume, VOLUME_NORM_MULTIPLIER, null));
    }

    public static void addRescaledIndicatorFeatues(String type, String suffix, Timeframe tfm,
                                                   Features.Builder features) {
        addRescaledFloatFeaturesList(type + "rangeHigh" + suffix, tfm.rangeHighs.highs, features);
        features.putFeature(type + "rangeHighProximity5pCounts" + suffix,
                createFeature(tfm.rangeHighs.proximity5pCounts));
        features.putFeature(type + "rangeHighProximity10pCounts" + suffix,
                createFeature(tfm.rangeHighs.proximity10pCounts));
        features.putFeature(type + "rangeHighProximity20pCounts" + suffix,
                createFeature(tfm.rangeHighs.proximity20pCounts));
        addRescaledFloatFeaturesList(type + "rangeHighPercentiles" + suffix, tfm.rangeHighs.percentiles, features);
        features.putFeature(type + "rangeHighLinearA" + suffix, createFeature(tfm.rangeHighs.linearA));
        features.putFeature(type + "rangeHighLinearB" + suffix, createFeature(tfm.rangeHighs.linearB));

        addRescaledFloatFeaturesList(type + "rangeLow" + suffix, tfm.rangeLows.lows, features);
        features.putFeature(type + "rangeLowProximity5pCounts" + suffix,
                createFeature(tfm.rangeLows.proximity5pCounts));
        features.putFeature(type + "rangeLowProximity10pCounts" + suffix,
                createFeature(tfm.rangeLows.proximity10pCounts));
        features.putFeature(type + "rangeLowProximity20pCounts" + suffix,
                createFeature(tfm.rangeLows.proximity20pCounts));
        addRescaledFloatFeaturesList(type + "rangeLowPercentiles" + suffix, tfm.rangeLows.percentiles, features);
        features.putFeature(type + "rangeLowLinearA" + suffix, createFeature(tfm.rangeLows.linearA));
        features.putFeature(type + "rangeLowLinearB" + suffix, createFeature(tfm.rangeLows.linearB));
    }

    public static void addRescaledFloatFeaturesList(String feature, List<Float> l, Features.Builder features) {
        features.putFeature(feature, createFeature(l));
        features.putFeature(feature + "_discrete", createRescaledDescreteFeature(l));
    }

    static Feature createRescaledDescreteFeature(List<Float> l) {
        List<Long> longList = new ArrayList<>();
        for (var f : l) {
            var longValue = Math.round(f.doubleValue());
            if (longValue < 0) {
                longValue = 0;
            }
            if (longValue > 39) {
                longValue = 39;
            }
            longList.add(longValue);
        }
        return Feature.newBuilder().setInt64List(
                Int64List.newBuilder().addAllValue(longList)).build();
    }

    public static void addTimeframeHistogramFeatures(String type, String suffix, TimeframeCandles values, float base,
                                                     Features.Builder features) {
        suffix = suffix + "_histogram";
        features.putFeature(type + "open" + suffix, createHistogramFeature(values.open, base));
        features.putFeature(type + "high" + suffix, createHistogramFeature(values.high, base));
        features.putFeature(type + "low" + suffix, createHistogramFeature(values.low, base));
        features.putFeature(type + "close" + suffix, createHistogramFeature(values.close, base));
    }

    public static Feature createHistogramFeature(List<Float> l, float base) {
        List<Float> res = new ArrayList<>();
        for (var f : l) {
            for (int i = 0; i < 20; i ++) {
                if (f <= base - PROFIT_HISTOGRAM_STEP * i) {
                    res.add(1f);
                } else {
                    res.add(0f);
                }
            }
            for (int i = 0; i < 20; i ++) {
                if (f >= base + PROFIT_HISTOGRAM_STEP * i) {
                    res.add(1f);
                } else {
                    res.add(0f);
                }
            }
        }
        return Feature.newBuilder().setFloatList(
                FloatList.newBuilder().addAllValue(res)).build();
    }

    public static Feature createFeature(List<Float> l) {
        return createFeature(l, null, null);
    }

    public static Feature createFeature(List<Float> l, Float mul, Float add) {
        if (mul != null) {
            l = l.stream()
                    .map(f -> f * mul)
                    .collect(Collectors.toList());
        }
        if (add != null) {
            l = l.stream()
                    .map(f -> f + add)
                    .collect(Collectors.toList());
        }
        return Feature.newBuilder().setFloatList(
                FloatList.newBuilder().addAllValue(l)).build();
    }

    public static Feature createFeature(Long l) {
        return Feature.newBuilder().setInt64List(
                Int64List.newBuilder().addValue(l)).build();
    }

    public static Timeframes createPrecedingTimeframes(Handle h, long epochMin, List<Timepoint> askTimepoints,
                                                       List<Timepoint> bidTimepoints) {
        var timeframes = new Timeframes();
        timeframes.ask = createPrecedingTimeframe(h, epochMin, "ask", askTimepoints);
        timeframes.bid = createPrecedingTimeframe(h, epochMin, "bid", bidTimepoints);
        if (timeframes.ask == null || timeframes.bid == null) {
            return null;
        }
        return timeframes;
    }

    public static Timeframe createPrecedingTimeframe(Handle h, long epochMin, String type,
                                                     List<Timepoint> timepoints) {
        NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_" + type + "_total_requested");
        var tfm = new Timeframe();
        tfm.timepoints = timepoints;
        if (tfm.timepoints.size() == 0) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_" + type + "_total_skipped");
            return null;
        }
        if (tfm.timepoints.size() != 1800) {
            var newTimepoints = new ArrayList<Timepoint>();
            for (int i = 0; i < 1800 - tfm.timepoints.size(); i ++) {
                newTimepoints.add(SerializationUtils.clone(tfm.timepoints.get(0)));
            }
            newTimepoints.addAll(tfm.timepoints);
            tfm.timepoints = newTimepoints;
        }
        tfm.initVectors();
        NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_" + type + "_total_emitted");
        return tfm;
    }

    public static Timeframes createNextTimeframes(Handle h, long epochMin, List<Timepoint> askTimepoints,
                                                  List<Timepoint> bidTimepoints) {
        var timeframes = new Timeframes();
        timeframes.ask = createNextTimeframe(h, epochMin, "ask", askTimepoints);
        timeframes.bid = createNextTimeframe(h, epochMin, "bid", bidTimepoints);
        if (timeframes.ask == null || timeframes.bid == null) {
            return null;
        }
        return timeframes;
    }

    public static Timeframe createNextTimeframe(Handle h, long epochMin, String type,
                                                List<Timepoint> timepoints) {
        NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_total_requested");
        // System.out.println("CreateTfRecordFile.createNextTimeframe epochMin = " + epochMin);
        if (timepoints.size() < NEXT_M1_TMF_SIZE) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_smaller");
        }
        if (timepoints.size() > NEXT_M1_TMF_SIZE) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_larger");
        }
        if (timepoints.size() != NEXT_M1_TMF_SIZE) {
            // System.out.println("CreateTfRecordFile.createNextTimeframe dbRes.list().size() = " + dbRes.list().size());
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_mismatch");
            return null;
        };
        var tfm = new Timeframe();
        tfm.timepoints = timepoints;
        tfm.initVectors();
        if (tfm.candles.volume.contains(0f)) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_zero_volume");
            return null;
        }
        NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_total_emitted");
        return tfm;
    }

    public static Timeframes aggregateTimeframes(Timeframes src, int granularity) {
        var timeframes = new Timeframes();
        timeframes.ask = aggregateTimeframe(src.ask, granularity);
        timeframes.bid = aggregateTimeframe(src.bid, granularity);
        return timeframes;
    }

    public static Timeframe aggregateTimeframe(Timeframe src, int granularity) {
        var res = new Timeframe();
        for (int i = 0; i < src.timepoints.size() / granularity; i ++) {
            var timepoints = src.timepoints.subList(i * granularity,
                    (i + 1) * granularity);
            var newTp = new Timepoint();
            newTp.open = timepoints.get(0).open;
            newTp.close = timepoints.get(granularity - 1).close;
            newTp.high = timepoints.stream().map(t -> t.high)
                    .max((f1, f2) -> Float.compare(f1, f2))
                    .get();
            newTp.low = timepoints.stream().map(t -> t.low)
                    .min((f1, f2) -> Float.compare(f1, f2))
                    .get();
            newTp.volume = 0;
            for (var tp : timepoints) {
                newTp.volume += tp.volume;
            }
            res.timepoints.add(newTp);
        }
        res.initVectors();
        return res;
    }
}
