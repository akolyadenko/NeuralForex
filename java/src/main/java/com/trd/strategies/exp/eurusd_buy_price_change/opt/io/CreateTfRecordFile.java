// ./gradlew -PmainClass=com.trd.strategies.exp.eurusd_buy.opt.CreateTfRecordFile run
package com.trd.strategies.exp.eurusd_buy_price_change.opt.io;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import com.trd.db.Db;
import com.trd.util.NiStats;
import com.trd.util.TfUtil;
import org.apache.commons.lang3.SerializationUtils;
import org.jdbi.v3.core.Handle;
import org.tensorflow.example.*;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CreateTfRecordFile {
    // EURUSD
    static int NEXT_M1_TMF_SIZE = 240;
    static float BUY_SELL_SPREAD = 0.0002f;
    static float VOLUME_NORM_MULTIPLIER = 1f;
    static float PROFIT_HISTOGRAM_STEP = 0.0001f;

    public static void main(String[] args) throws Exception {
        System.out.println("CreateTfRecordFile.main starting.");
        NiStats.printEvery5Second();
        createSplit("train");
        createSplit("test");
        createSplit("eval");
        NiStats.stopAndPrint();
    }

    private static void createSplit(String split) throws Exception {
        DataOutputStream out = createSplitOut(split);
        var executorService = Executors.newFixedThreadPool(16);
        try (var h = Db.jdbi().open()) {
            var dbRes = h.createQuery("""
                select 
                    epoch_min,
                    year,
                    month,
                    day,
                    hour,
                    minute 
                from example
                join timepoint using (epoch_min)
                where split = :split
                order by random()
            """)
                    .bind("split", split)
                    .mapToMap();
            for (var row : dbRes) {
                var epochMin = (Long)row.get("epoch_min");
                var year = (Long)row.get("year");
                var month = (Long)row.get("month");
                var day = (Long)row.get("day");
                var hour = (Long)row.get("hour");
                var minute = (Long)row.get("minute");
                executorService.submit(() -> createAndWriteExample(split, out, epochMin, year, month, day,
                        hour, minute));
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(1000, TimeUnit.DAYS);
        out.close();
    }

    private static DataOutputStream createSplitOut(String split) throws FileNotFoundException {
        DataOutputStream out = new DataOutputStream(new FileOutputStream("/usr/proj/trd/" + split + ".tfrecord"));
        return out;
    }

    private static void createAndWriteExample(String split, DataOutputStream out, Long epochMin, Long year,
                                              Long month, Long day, Long hour, Long minute) {
        try {
            var example = Example.newBuilder();
            var features = example.getFeaturesBuilder();
            if (!buildExample(epochMin, features)) {
                return;
            }
            features.putFeature("epoch_min", createFeature(epochMin));
            features.putFeature("year", createFeature(year));
            features.putFeature("month", createFeature(month));
            features.putFeature("day", createFeature(day));
            features.putFeature("day_of_week", createFeature((epochMin / (60 * 24)) % 7));
            features.putFeature("hour", createFeature(hour));
            features.putFeature("minute", createFeature(minute));
            // System.out.println("CreateTfRecordFile.main example = " + example);
            var bytes = example.build().toByteArray();
            synchronized (out) {
                TfUtil.write(out, bytes);
            }
            NiStats.counters.incrementAndGet("main_examples_total_created");
            NiStats.counters.incrementAndGet("main_examples_" + split + "_created");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    static boolean buildExample(long epochMin, Features.Builder features) {
        try (var h = Db.jdbi().open()) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.example_requested");
            var m1tfm = createPrecedingTimeframe(h, epochMin, "m1");
            var nextm1tfm = createNextTimeframe(h, epochMin);
            if (m1tfm == null || nextm1tfm == null) {
                NiStats.counters.incrementAndGet("CreateTfRecordFile.example_skipped");
                return false;
            }
            var m5tfm = aggregateTimeframe(m1tfm, 5);
            var m15tfm = aggregateTimeframe(m1tfm, 15);
            var m30tfm = aggregateTimeframe(m1tfm, 30);
            var base = m1tfm.timepoints.get(m1tfm.timepoints.size() - 1).close;
            var maxVolume = m1tfm.maxVolume();
            m1tfm.candles.takeLast(60);
            m1tfm.buildRescaled(base, maxVolume);
            m1tfm.buildIndicators();
            m5tfm.candles.takeLast(60);
            m5tfm.buildRescaled(base, maxVolume);
            m5tfm.buildIndicators();
            m15tfm.candles.takeLast(60);
            m15tfm.buildRescaled(base, maxVolume);
            m15tfm.buildIndicators();
            m30tfm.candles.takeLast(60);
            m30tfm.buildRescaled(base, maxVolume);
            m30tfm.buildIndicators();
            NiStats.counters.incrementAndGet("CreateTfRecordFile.example_created");
            addTimeframeFeatures("m1", m1tfm, base, features);
            addTimeframeFeatures("m5", m5tfm, base, features);
            addTimeframeFeatures("m15", m15tfm, base, features);
            addTimeframeFeatures("m30", m30tfm, base, features);
            // addTimeframeFeatures("nextm1", nextm1tfm, base, features);
            for (var mins : Lists.newArrayList(5, 15, 30, 60, 120, 240)) {
                if (!addPriceIncreaseHistogram(nextm1tfm, features, mins)) {
                    return false;
                }
                if (!addPriceDecreaseHistogram(nextm1tfm, features, mins)) {
                    return false;
                }
                if (!addPriceChangeFeatures(nextm1tfm, features, mins)) {
                    return false;
                }
            }
            addHighLow(nextm1tfm, base, features);
            return true;
        }
    }

    static boolean addPriceChangeFeatures(Timeframe nextm1tfm, Features.Builder features, int mins) {
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

    static boolean addPriceIncreaseHistogram(Timeframe nextm1tfm, Features.Builder features, int mins) {
        Preconditions.checkArgument(nextm1tfm.timepoints.size() >= mins);
        var timepoints = nextm1tfm.timepoints.subList(0, mins);
        float buyPrice = timepoints.get(0).open + BUY_SELL_SPREAD;
        float maxLow = timepoints
                .subList(1, timepoints.size())
                .stream()
                .map(pnt -> pnt.low)
                .max(Floats::compare)
                .get();
        var histogram = new ArrayList<Float>();
        for (int histIdx = 0; histIdx < 100; histIdx++) {
            float profitStop = buyPrice + histIdx * PROFIT_HISTOGRAM_STEP;
            if (profitStop <= maxLow) {
                histogram.add(1f);
            } else {
                histogram.add(0f);
            }
        }
        features.putFeature("price_increase_histogram_" + mins, createFeature(histogram));
        return true;
    }

    static boolean addPriceDecreaseHistogram(Timeframe nextm1tfm, Features.Builder features, int mins) {
        Preconditions.checkArgument(nextm1tfm.timepoints.size() >= mins);
        var timepoints = nextm1tfm.timepoints.subList(0, mins);
        float sellPrice = timepoints.get(0).open - BUY_SELL_SPREAD;
        float minHigh = timepoints
                .subList(1, timepoints.size())
                .stream()
                .map(pnt -> pnt.high)
                .min(Floats::compare)
                .get();
        var histogram = new ArrayList<Float>();
        for (int histIdx = 0; histIdx < 100; histIdx++) {
            float profitStop = sellPrice - histIdx * PROFIT_HISTOGRAM_STEP;
            if (profitStop >= minHigh) {
                histogram.add(1f);
            } else {
                histogram.add(0f);
            }
        }
        features.putFeature("price_decrease_histogram_" + mins, createFeature(histogram));
        return true;
    }

    static boolean addHighLow(Timeframe nextm1tfm, float base, Features.Builder features) {
        var highs = new ArrayList<Float>();
        var lows = new ArrayList<Float>();
        for (int mins = 5; mins <= 120; mins += 5) {
            var timepoints = nextm1tfm.timepoints.subList(0, mins);
            highs.add(timepoints.stream()
                    .map(t -> (t.high - base) * 100).max((f1, f2) -> Floats.compare(f1, f2)).get());
            lows.add(timepoints.stream()
                    .map(t -> (t.low - base) * 100).min((f1, f2) -> Floats.compare(f1, f2)).get());
        }
        features.putFeature("next_tfm_highs", createFeature(highs));
        features.putFeature("next_tfm_lows", createFeature(lows));
        return true;
    }

    static void addTimeframeFeatures(String type, Timeframe tfm, float base, Features.Builder features) {
        // addTimeframeFeatures(type, "", tfm.candles, features);
        addCandleFeatures(type, "_rescaled", tfm.rescaledCandles, features);
        addIndicatorFeatues(type, "_rescaled", tfm, features);

        // addTimeframeHistogramFeatures(type, "_rescaled", tfm.rescaledValues, base, features);
    }

    static void addCandleFeatures(String type, String suffix, Timeframe.TimeframeCandles values,
                                  Features.Builder features) {
        features.putFeature(type + "open" + suffix, createFeature(values.open));
        features.putFeature(type + "high" + suffix, createFeature(values.high));
        features.putFeature(type + "low" + suffix, createFeature(values.low));
        features.putFeature(type + "close" + suffix, createFeature(values.close));
        features.putFeature(type + "volume" + suffix, createFeature(values.volume, VOLUME_NORM_MULTIPLIER, null));
    }

    static void addIndicatorFeatues(String type, String suffix, Timeframe tfm,
                                  Features.Builder features) {
        features.putFeature(type + "rangeHigh" + suffix, createFeature(tfm.rangeHighs.highs));
        features.putFeature(type + "rangeHighProximity5pCounts" + suffix,
                createFeature(tfm.rangeHighs.proximity5pCounts));
        features.putFeature(type + "rangeHighProximity10pCounts" + suffix,
                createFeature(tfm.rangeHighs.proximity10pCounts));
        features.putFeature(type + "rangeHighProximity20pCounts" + suffix,
                createFeature(tfm.rangeHighs.proximity20pCounts));
        features.putFeature(type + "rangeHighLinearA" + suffix, createFeature(tfm.rangeHighs.linearA));
        features.putFeature(type + "rangeHighLinearB" + suffix, createFeature(tfm.rangeHighs.linearB));

        features.putFeature(type + "rangeLow" + suffix, createFeature(tfm.rangeLows.lows));
        features.putFeature(type + "rangeLowProximity5pCounts" + suffix,
                createFeature(tfm.rangeLows.proximity5pCounts));
        features.putFeature(type + "rangeLowProximity10pCounts" + suffix,
                createFeature(tfm.rangeLows.proximity10pCounts));
        features.putFeature(type + "rangeLowProximity20pCounts" + suffix,
                createFeature(tfm.rangeLows.proximity20pCounts));
        features.putFeature(type + "rangeLowLinearA" + suffix, createFeature(tfm.rangeLows.linearA));
        features.putFeature(type + "rangeLowLinearB" + suffix, createFeature(tfm.rangeLows.linearB));
    }

    static void addTimeframeHistogramFeatures(String type, String suffix, Timeframe.TimeframeCandles values, float base,
                                              Features.Builder features) {
        suffix = suffix + "_histogram";
        features.putFeature(type + "open" + suffix, createHistogramFeature(values.open, base));
        features.putFeature(type + "high" + suffix, createHistogramFeature(values.high, base));
        features.putFeature(type + "low" + suffix, createHistogramFeature(values.low, base));
        features.putFeature(type + "close" + suffix, createHistogramFeature(values.close, base));
    }

    static Feature createHistogramFeature(List<Float> l, float base) {
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

    static Feature createFeature(List<Float> l) {
        return createFeature(l, null, null);
    }

    static Feature createFeature(List<Float> l, Float mul, Float add) {
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

    static Feature createFeature(Long l) {
        return Feature.newBuilder().setInt64List(
                Int64List.newBuilder().addValue(l)).build();
    }

    static Timeframe createPrecedingTimeframe(Handle h, long epochMin, String type) {
        NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_" + type + "_total_requested");
        var epochDiff = 2000;
        var dbRes = h.createQuery("""
            select *
            from (
                select * 
                from timeframe
                where type = :type
                    and epoch_min < :epochMin
                    and epoch_min >= :epochMin - :epochDiff
                order by epoch_min desc
                limit 1800
            ) data
            order by epoch_min
        """)
                .bind("type", type)
                .bind("epochMin", epochMin)
                .bind("epochDiff", epochDiff)
                .mapToMap();
        var tfm = new Timeframe();
        for (var row : dbRes) {
            var tpnt = new Timeframe.Timepoint(row);
            tfm.timepoints.add(tpnt);
        }
        if (tfm.timepoints.size() == 0) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_" + type + "_total_skipped");
            return null;
        }
        if (tfm.timepoints.size() != 1800) {
            var newTimepoints = new ArrayList<Timeframe.Timepoint>();
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

    static Timeframe createNextTimeframe(Handle h, long epochMin) {
        NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_total_requested");
        // System.out.println("CreateTfRecordFile.createNextTimeframe epochMin = " + epochMin);
        var dbRes = h.createQuery("""
            select *
            from (
                select * 
                from timeframe
                where type = 'm1'
                    and epoch_min >= :epochMin
                    and epoch_min < :epochMin + :nextTmfSize
                    and generated = false
                order by epoch_min desc
            ) data
            order by epoch_min
        """)
                .bind("epochMin", epochMin)
                .bind("nextTmfSize", NEXT_M1_TMF_SIZE)
                .mapToMap();
        if (dbRes.list().size() < NEXT_M1_TMF_SIZE) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_smaller");
        }
        if (dbRes.list().size() > NEXT_M1_TMF_SIZE) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_larger");
        }
        if (dbRes.list().size() != NEXT_M1_TMF_SIZE) {
            // System.out.println("CreateTfRecordFile.createNextTimeframe dbRes.list().size() = " + dbRes.list().size());
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_mismatch");
            return null;
        };
        var tfm = new Timeframe();
        for (var row : dbRes) {
            var tpnt = new Timeframe.Timepoint(row);
            tfm.timepoints.add(tpnt);
        }
        tfm.initVectors();
        if (tfm.candles.volume.contains(0f)) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_zero_volume");
            return null;
        }
        NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_total_emitted");
        return tfm;
    }

    static Timeframe aggregateTimeframe(Timeframe src, int granularity) {
        var res = new Timeframe();
        for (int i = 0; i < src.timepoints.size() / granularity; i ++) {
            var timepoints = src.timepoints.subList(i * granularity,
                    (i + 1) * granularity);
            var newTp = new Timeframe.Timepoint();
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
