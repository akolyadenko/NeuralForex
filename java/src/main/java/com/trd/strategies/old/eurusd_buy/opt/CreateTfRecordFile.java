// ./gradlew -PmainClass=CreateTfRecordFile run
package com.trd.strategies.old.eurusd_buy.opt;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import com.trd.db.Db;
import com.trd.util.NiStats;
import com.trd.util.TfUtil;
import org.jdbi.v3.core.Handle;
import org.tensorflow.example.*;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CreateTfRecordFile {
    // EURUSD
    static int NEXT_M1_TMF_SIZE = 30;
    static float BUY_SELL_SPREAD = 0.0001f;
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
                executorService.submit(() -> createAndWriteExample(split, out, epochMin, year, month, day, hour, minute));
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
        var example = Example.newBuilder();
        var features = example.getFeaturesBuilder();
        if (!buildExample(epochMin, features)) {
            return;
        }
        features.putFeature("epoch_min", createFeature(epochMin));
        features.putFeature("year", createFeature(year));
        features.putFeature("month", createFeature(month));
        features.putFeature("day", createFeature(day));
        features.putFeature("hour", createFeature(hour));
        features.putFeature("minute", createFeature(minute));
        // System.out.println("CreateTfRecordFile.main example = " + example);
        var bytes = example.build().toByteArray();
        synchronized (out) {
            TfUtil.write(out, bytes);
        }
        NiStats.counters.incrementAndGet("main_examples_total_created");
        NiStats.counters.incrementAndGet("main_examples_" + split + "_created");
    }

    static boolean buildExample(long epochMin, Features.Builder features) {
        try (var h = Db.jdbi().open()) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.example_requested");
            var m1tfm = createPrecedingTimeframe(h, epochMin, "m1");
            var m5tfm = createPrecedingTimeframe(h, epochMin, "m5");
            var m15tfm = createPrecedingTimeframe(h, epochMin, "m15");
            var m30tfm = createPrecedingTimeframe(h, epochMin, "m30");
            // var h1tfm = createPrecedingTimeframe(h, epochMin, "h1");
            // var h4tfm = createPrecedingTimeframe(h, epochMin, "h4");
            var nextm1tfm = createNextTimeframe(h, epochMin);
            if (m1tfm == null || m5tfm == null || m15tfm == null || m30tfm == null ||
                    // h1tfm == null || h4tfm == null ||
                    nextm1tfm == null) {
                NiStats.counters.incrementAndGet("CreateTfRecordFile.example_skipped");
                return false;
            }
            var base = m1tfm.timepoints.get(59).close;
            var maxVolume = Collections.max(Lists.newArrayList(
                    m1tfm.maxVolume(), m5tfm.maxVolume(), m15tfm.maxVolume(), m30tfm.maxVolume()));
            m1tfm.buildRescaled(base, maxVolume);
            m5tfm.buildRescaled(base, maxVolume);
            m15tfm.buildRescaled(base, maxVolume);
            m30tfm.buildRescaled(base, maxVolume);
            NiStats.counters.incrementAndGet("CreateTfRecordFile.example_created");
            addTimeframeFeatures("m1", m1tfm, features);
            addTimeframeFeatures("m5", m5tfm, features);
            addTimeframeFeatures("m15", m15tfm, features);
            addTimeframeFeatures("m30", m30tfm, features);
            // addTimeframeFeatures("h1", h1tfm, features);
            // addTimeframeFeatures("h4", h4tfm, features);
            addTimeframeFeatures("nextm1", nextm1tfm, features);
            if (!addPriceIncreaseHistogram(nextm1tfm, features)) {
                return false;
            }
            if (!addPriceDecreaseHistogram(nextm1tfm, features)) {
                return false;
            }
            return true;
        }
    }

    static boolean addPriceIncreaseHistogram(Timeframe nextm1tfm, Features.Builder features) {
        Preconditions.checkArgument(nextm1tfm.timepoints.size() == NEXT_M1_TMF_SIZE);
        float buyPrice = nextm1tfm.timepoints.get(0).high + BUY_SELL_SPREAD;
        float maxLow = nextm1tfm.timepoints
                .subList(1,nextm1tfm.timepoints.size())
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
        features.putFeature("price_increase_histogram", createFeature(histogram));
        return true;
    }

    static boolean addPriceDecreaseHistogram(Timeframe nextm1tfm, Features.Builder features) {
        Preconditions.checkArgument(nextm1tfm.timepoints.size() == NEXT_M1_TMF_SIZE);
        float sellPrice = nextm1tfm.timepoints.get(0).low - BUY_SELL_SPREAD;
        float minHigh = nextm1tfm.timepoints
                .subList(1,nextm1tfm.timepoints.size())
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
        features.putFeature("price_decrease_histogram", createFeature(histogram));
        return true;
    }

    static void addTimeframeFeatures(String type, Timeframe tfm, Features.Builder features) {
        addTimeframeFeatures(type, "", tfm.values, features);
        addTimeframeFeatures(type, "_rescaled", tfm.rescaledValues, features);
    }

    static void addTimeframeFeatures(String type, String suffix, TimeframeValues values, Features.Builder features) {
        features.putFeature(type + "open" + suffix, createFeature(values.open, null, null));
        features.putFeature(type + "high" + suffix, createFeature(values.high, null, null));
        features.putFeature(type + "low" + suffix, createFeature(values.low, null, null));
        features.putFeature(type + "close" + suffix, createFeature(values.close, null, null));
        features.putFeature(type + "volume" + suffix, createFeature(values.volume, VOLUME_NORM_MULTIPLIER, null));
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
        var epochDiff = 0L;
        if (type.equals("m1")) {
            epochDiff = 80;
        } else if (type.equals("m5")) {
            epochDiff = 80 * 5;
        } else if (type.equals("m15")) {
            epochDiff = 80 * 15;
        } else if (type.equals("m30")) {
            epochDiff = 80 * 30;
        } else if (type.equals("h1")) {
            epochDiff = 80 * 60;
        } else if (type.equals("h4")) {
            epochDiff = 80 * 240;
        } else {
            throw new RuntimeException("Unknown type " + type);
        }
        var dbRes = h.createQuery("""
            select *
            from (
                select * 
                from timeframe
                where type = :type
                    and epoch_min < :epochMin
                    and epoch_min >= :epochMin - :epochDiff
                order by epoch_min desc
                limit 60
            ) data
            order by epoch_min
        """)
                .bind("type", type)
                .bind("epochMin", epochMin)
                .bind("epochDiff", 70 * 60 * 4)
                .mapToMap();
        if (dbRes.list().size() != 60) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_" + type + "_skipped");
            return null;
        };
        var tfm = new Timeframe();
        for (var row : dbRes) {
            var tpnt = new Timepoint(row);
            tfm.timepoints.add(tpnt);
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
                order by epoch_min desc
            ) data
            order by epoch_min
        """)
                .bind("epochMin", epochMin)
                .bind("nextTmfSize", NEXT_M1_TMF_SIZE)
                .mapToMap();
        if (dbRes.list().size() != NEXT_M1_TMF_SIZE) {
            // System.out.println("CreateTfRecordFile.createNextTimeframe dbRes.list().size() = " + dbRes.list().size());
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_mismatch");
            return null;
        };
        var tfm = new Timeframe();
        for (var row : dbRes) {
            var tpnt = new Timepoint(row);
            tfm.timepoints.add(tpnt);
        }
        tfm.initVectors();
        if (tfm.values.volume.contains(0f)) {
            NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_skipped_size_zero_volume");
            return null;
        }
        NiStats.counters.incrementAndGet("CreateTfRecordFile.timeframe_next_total_emitted");
        return tfm;
    }

    static class Timepoint {
        public float open, high, low, close, volume;

        Timepoint(Map<String, Object> row) {
            open = (Float)row.get("open");
            high = (Float)row.get("high");
            low = (Float)row.get("low");
            close = (Float)row.get("close");
            volume = (Float)row.get("volume");
        }
    }

    static class TimeframeValues {
        public List<Float> open = new ArrayList<>();
        public List<Float> high = new ArrayList<>();
        public List<Float> low = new ArrayList<>();
        public List<Float> close = new ArrayList<>();
        public List<Float> volume = new ArrayList<>();

        TimeframeValues buildRescaled(float buyPrice, float maxVolume) {
            var rescaled = new TimeframeValues();
            rescaled.open = rescalePrice(open, buyPrice);
            rescaled.high = rescalePrice(high, buyPrice);
            rescaled.low = rescalePrice(low, buyPrice);
            rescaled.close = rescalePrice(close, buyPrice);
            rescaled.volume = rescaleVolume(volume, maxVolume);
            return rescaled;
        }

        List<Float> rescalePrice(List<Float> l, float buy) {
            return l.stream()
                    .map(v -> v - buy)
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
    }

    static class Timeframe {
        public List<Timepoint> timepoints = new ArrayList<>();
        public TimeframeValues values = new TimeframeValues();
        public TimeframeValues rescaledValues = new TimeframeValues();

        public void initVectors() {
            for (var tpnt : timepoints) {
                values.open.add(tpnt.open);
                values.high.add(tpnt.high);
                values.low.add(tpnt.low);
                values.close.add(tpnt.close);
                values.volume.add(tpnt.volume);
            }
        }

        public float minPrice() {
            var prices = new ArrayList<Float>();
            prices.addAll(values.open);
            prices.addAll(values.high);
            prices.addAll(values.low);
            prices.addAll(values.close);
            return Collections.min(prices);
        }

        public float maxVolume() {
            return Collections.max(values.volume);
        }

        public void buildRescaled(float buyPrice, float maxVolume) {
            rescaledValues = values.buildRescaled(buyPrice, maxVolume);
        }
    }
}
