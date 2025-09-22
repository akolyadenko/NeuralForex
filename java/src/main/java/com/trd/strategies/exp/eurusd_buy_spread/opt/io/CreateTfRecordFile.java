// ./gradlew -PmainClass=com.trd.strategies.exp.eurusd_buy.opt.CreateTfRecordFile run
package com.trd.strategies.exp.eurusd_buy_spread.opt.io;

import com.trd.db.Db;
import com.trd.strategies.exp.eurusd_buy_spread.opt.io.timeframe.Timeframe;
import com.trd.strategies.exp.eurusd_buy_spread.opt.io.timeframe.Timepoint;
import com.trd.util.NiStats;
import com.trd.util.TfUtil;
import org.tensorflow.example.*;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CreateTfRecordFile {
    public static void main(String[] args) throws Exception {
        System.out.println("CreateTfRecordFile.main starting.");
        NiStats.printEvery5Second();
        createSplit("train");
        createSplit("test");
        createSplit("eval1");
        createSplit("eval2");
        createSplit("eval3");
        createSplit("eval4");
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
            if (!ExampleBuilder.buildExample(epochMin, year, month, day, hour, minute,
                    readPreceedingTimepointsFromDb(epochMin, "ask"),
                    readPreceedingTimepointsFromDb(epochMin, "bid"),
                    readNextTimepointsFromDb(epochMin, "ask"),
                    readNextTimepointsFromDb(epochMin, "bid"),
                    features)) {
                return;
            }
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

    public static List<Timepoint> readPreceedingTimepointsFromDb(long epochMin, String type) {
        try (var h = Db.jdbi().open()) {
            var epochDiff = 2000;
            var timepoints = new ArrayList<Timepoint>();
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
            for (var row : dbRes) {
                var tpnt = new Timepoint(row);
                timepoints.add(tpnt);
            }
            return timepoints;
        }
    }

    public static ArrayList<Timepoint> readNextTimepointsFromDb(long epochMin, String type) {
        try (var h = Db.jdbi().open()) {
            var dbRes = h.createQuery("""
                select *
                from (
                    select * 
                    from timeframe
                    where type = :type
                        and epoch_min >= :epochMin
                        and epoch_min < :epochMin + :nextTmfSize
                        and generated = false
                    order by epoch_min desc
                ) data
                order by epoch_min
            """)
                .bind("type", type)
                .bind("epochMin", epochMin)
                .bind("nextTmfSize", ExampleBuilder.NEXT_M1_TMF_SIZE)
                .mapToMap();
        var timepoints = new ArrayList<Timepoint>();
        for (var row : dbRes) {
            var tpnt = new Timepoint(row);
            timepoints.add(tpnt);
        }
        return timepoints;
        }
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
