// ./gradlew -PmainClass=com.trd.strategies.exp.eurusd_buy.opt.ExampleWalker run
package com.trd.strategies.exp.eurusd_buy_price_change.opt;

import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;
import com.trd.db.Db;
import com.trd.util.NiStats;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ExampleWalker {
    public static ExecutorService executorService = Executors.newFixedThreadPool(32);
    public static float COMMISSION = 0.0001f;
    public static float SPREAD = 0.0001f;

    static class CheckpointWalks {int checkpoint; Walk testWalk, evalWalk;}

    public static void main(String[] args) throws Exception {
        initDb();
        System.out.println("ExampleWalker.main starting.");
        List<Long> checkpoints;
        try (var h = Db.jdbi().open()) {
            checkpoints = h.createQuery("""
                select distinct checkpoint from example_prediction order by 1
            """)
                    .mapToMap()
                    .stream()
                    .map(r -> (Long)r.get("checkpoint"))
                    .collect(Collectors.toList());
        }
        var walks = checkpoints.stream()
                .map(c -> findBestWalkForCheckpoint(c))
                .collect(Collectors.toList());
        var checkpointWalks = new ArrayList<CheckpointWalks>();
        for (var w : walks) {
            var evalWalk = runWalk(w, loadSteps("eval", w.checkpoint));
            var checkpoint = new CheckpointWalks();
            checkpoint.testWalk = w;
            checkpoint.evalWalk = evalWalk;
            checkpointWalks.add(checkpoint);
        };
        Collections.sort(checkpointWalks, (w1, w2) -> Floats.compare(w1.evalWalk.adjustedProfit,
                w2.evalWalk.adjustedProfit));
        checkpointWalks.forEach(w ->
                System.out.println("ExampleWalker.main checkpont #" + w.testWalk.checkpoint
                        + " test profit: " + w.testWalk.profit
                        + " eval profit: " + w.evalWalk.profit + " trades: " + w.evalWalk.trades.size()));
        System.out.println("ExampleWalker.main walk:");
        var bestWalk = checkpointWalks.get(walks.size() -1);
        System.out.println("ExampleWalker.main best test walk:");
        bestWalk.testWalk.print();
        System.out.println("ExampleWalker.main best eval walk:");
        bestWalk.evalWalk.print();
        executorService.shutdown();
        NiStats.print();
    }

    static Walk findBestWalkForCheckpoint(long checkpoint) {
        try {
            var trainSteps = loadSteps("train", checkpoint);
            var testSteps = loadSteps("test", checkpoint);
            var evalSteps = loadSteps("eval", checkpoint);
            var walk = findBestWalk(trainSteps, testSteps, evalSteps);
            walk.checkpoint = checkpoint;
            System.out.println("==============================  walk for checkpoint #" + checkpoint);
            walk.print();
            return walk;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Walk findBestWalk(List<WalkStep> trainSteps, List<WalkStep> testSteps, List<WalkStep> evalSteps) throws Exception {
        var walks = new LinkedList<Future<Walk>>();
        final float startStopLossDelta = 0.002f;
        final float startTakeProfitDelta = 0.002f;
        for (var enterThreshold = 0f; enterThreshold < 4; enterThreshold += 0.2) {
            for (var exitThreshold = -2f; exitThreshold < 2f; exitThreshold += 0.2) {
                for (var enterBucket = 6; enterBucket < 80; enterBucket += 20) {
                    for (var exitBucket = 6; exitBucket < 80; exitBucket += 20) {
                        final var walkEnterThreashold = enterThreshold;
                        final var walkExitThreashold = exitThreshold;
                        final var walkEnterBucket = enterBucket;
                        final var walkExitBucket = exitBucket;
                        walks.add(executorService.submit(() -> runWalk(
                                walkEnterBucket, walkEnterThreashold, walkExitBucket,
                                walkExitThreashold, startStopLossDelta, startTakeProfitDelta, testSteps,
                                        false)));
                    }
                }
            }
        }
        var bestWalk = new Walk();
        bestWalk.profit = -1000000;
        bestWalk.adjustedProfit = -1000000;
        bestWalk.winProfit = 1;
        bestWalk.lossProfit = -1000000;
        bestWalk = findBestWalk(walks, bestWalk);
        var prevBestWalk = bestWalk;
        do {
            prevBestWalk = bestWalk;
            final var oldBestWalk = bestWalk;
                for (var delta = -0.5f; delta < +0.5f; delta += 0.01) {
                    final var walkDelta = delta;
                    walks.add(executorService.submit(() -> runWalk(
                            oldBestWalk.enterBucket,
                            oldBestWalk.enterThreshold + walkDelta,
                            oldBestWalk.exitBucket,
                            oldBestWalk.exitThreshold,
                            oldBestWalk.stopLossDelta,
                            oldBestWalk.takeProfitDelta,
                            testSteps,
                            false)));
                }
            bestWalk = findBestWalk(walks, bestWalk);
            final var oldBestWalk2 = bestWalk;
                for (var delta = -0.5f; delta < +0.5f; delta += 0.01) {
                    final var walkDelta = delta;
                    walks.add(executorService.submit(() -> runWalk(
                            oldBestWalk2.enterBucket,
                            oldBestWalk2.enterThreshold,
                            oldBestWalk.exitBucket,
                            oldBestWalk2.exitThreshold + walkDelta,
                            oldBestWalk2.stopLossDelta,
                            oldBestWalk2.takeProfitDelta,
                            testSteps,
                            false)));
                }
            bestWalk = findBestWalk(walks, bestWalk);
            final var oldBestWalk3 = bestWalk;
            for (var delta = -0.001f; delta < +0.001f && oldBestWalk3.stopLossDelta + delta < 0.005f
                    && oldBestWalk3.stopLossDelta + delta > 0.0004f;
                 delta += 0.0001) {
                if (oldBestWalk3.stopLossDelta + delta < 0.0004f) {
                    continue;
                }
                final var walkDelta = delta;
                walks.add(executorService.submit(() -> runWalk(
                        oldBestWalk3.enterBucket,
                        oldBestWalk3.enterThreshold,
                        oldBestWalk3.exitBucket,
                        oldBestWalk3.exitThreshold,
                        oldBestWalk3.stopLossDelta + walkDelta,
                        oldBestWalk3.takeProfitDelta,
                        testSteps, false)));
            }
            bestWalk = findBestWalk(walks, bestWalk);
            final var oldBestWalk4 = bestWalk;
            for (var delta = -0.001f; delta < +0.001f && oldBestWalk4.takeProfitDelta + delta < 0.005f
                    && oldBestWalk4.takeProfitDelta + delta > 0.0004f;
                 delta += 0.0001) {
                if (oldBestWalk4.takeProfitDelta + delta < 0.0004f) {
                    continue;
                }
                final var walkDelta = delta;
                walks.add(executorService.submit(() -> runWalk(
                        oldBestWalk4.enterBucket,
                        oldBestWalk4.enterThreshold,
                        oldBestWalk4.exitBucket,
                        oldBestWalk4.exitThreshold,
                        oldBestWalk4.stopLossDelta,
                        oldBestWalk4.takeProfitDelta + walkDelta,
                        testSteps, false)));
            }
            bestWalk = findBestWalk(walks, bestWalk);
            final var oldBestWalk5 = bestWalk;
            for (var enterBucket = 0; enterBucket < +80; enterBucket++) {
                var localEnterBucket = enterBucket;
                walks.add(executorService.submit(() -> runWalk(
                        localEnterBucket,
                        oldBestWalk5.enterThreshold,
                        oldBestWalk5.exitBucket,
                        oldBestWalk5.exitThreshold,
                        oldBestWalk5.stopLossDelta,
                        oldBestWalk5.takeProfitDelta,
                        testSteps, false)));
            }
            bestWalk = findBestWalk(walks, bestWalk);
            final var oldBestWalk6 = bestWalk;
            for (var exitBucket = 0; exitBucket < +80; exitBucket++) {
                var localExitBucket = exitBucket;
                walks.add(executorService.submit(() -> runWalk(
                        oldBestWalk6.enterBucket,
                        oldBestWalk6.enterThreshold,
                        localExitBucket,
                        oldBestWalk6.exitThreshold,
                        oldBestWalk6.stopLossDelta,
                        oldBestWalk6.takeProfitDelta,
                        testSteps, false)));
            }
            bestWalk = findBestWalk(walks, bestWalk);
        } while(bestWalk.adjustedProfit > prevBestWalk.adjustedProfit);
        return runWalk(bestWalk.enterBucket, bestWalk.enterThreshold,
                bestWalk.exitBucket, bestWalk.exitThreshold, bestWalk.stopLossDelta,
                bestWalk.takeProfitDelta, testSteps, false);
    }

    private static Walk findBestWalk(LinkedList<Future<Walk>> walks, Walk bestWalk) throws Exception {
        while (!walks.isEmpty()) {
            var walk = walks.poll().get();
            // System.out.println("ExampleWalker.findBestWalk trades = " + walk.trades.size());
            // System.out.println("ExampleWalker.findBestWalk wins = " + walk.wins);
            // System.out.println("ExampleWalker.findBestWalk losses = " + walk.losses);
            if (
                    walk.adjustedProfit > bestWalk.adjustedProfit
                            // Math.abs(walk.winProfit / walk.lossProfit)
                            //        > Math.abs(bestWalk.winProfit / bestWalk.lossProfit)
                            // && walk.winProfitsStats.stats.getPercentile(10) > 0.0001
                            // && walk.wins > walk.losses * 2
                            && walk.trades.size() > 120
                            && walk.stopLossDelta > 0.0004f
                            && walk.durationsStats.stats.getPercentile(90) < 240
            ) {
                bestWalk = walk;
                // bestWalk.print();
            }
            // walk.print();
        }
        return bestWalk;
    }

    private static Walk runWalk(Walk walk, List<WalkStep> steps) {
        return runWalk(walk.enterBucket, walk.enterThreshold, walk.exitBucket, walk.exitThreshold,
                walk.stopLossDelta, walk.takeProfitDelta, steps, false);
    }

    private static Walk runWalk(int enterBucket, float enterThreshold, int exitBucket,
                                float exitThreshold, float stopLossDelta, float takeProfitDelta,
                                List<WalkStep> steps,
                                boolean logToDb) {
        var walk = new Walk();
        walk.enterBucket = enterBucket;
        walk.enterThreshold = enterThreshold;
        walk.exitBucket = exitBucket;
        walk.exitThreshold = exitThreshold;
        walk.stopLossDelta = stopLossDelta;
        walk.takeProfitDelta = takeProfitDelta;
        boolean inTrade = false;
        float buyPrice = 0;
        WalkTrade trade = null;
        for (var step : steps) {
            if (trade != null) {
                trade.minPrice = Math.min(trade.minPrice, step.low - SPREAD);
            }
            if (!inTrade && step.predictions.get(enterBucket) > enterThreshold) {
                inTrade = true;
                buyPrice = step.open;
                trade = new WalkTrade();
                trade.buyPrice = buyPrice;
                // trade.stopLoss = buyPrice - 0.0015f;
                trade.stopLoss = buyPrice - stopLossDelta;
                trade.takeProfit = buyPrice + takeProfitDelta;
                trade.minPrice = step.low - SPREAD;
                trade.enterEpochMin = step.epochMin;
            } else if (inTrade && (
                    step.predictions.get(exitBucket) < exitThreshold
                            || step.low - SPREAD <= trade.stopLoss
                            || step.high - SPREAD >= trade.takeProfit
                            || step.epochMin - trade.enterEpochMin > 240
            )) {
                inTrade = false;
                if (step.epochMin - trade.enterEpochMin > 241) {
                    walk.dataGaps ++;
                    trade = null;
                    continue;
                }
                trade.exitEpochMin = step.epochMin;
                var sellPrice = step.open - SPREAD;
                if (step.low - SPREAD <= trade.stopLoss) {
                    sellPrice = trade.stopLoss;
                    walk.stopLosses ++;
                }
                if (step.high - SPREAD >= trade.takeProfit) {
                    sellPrice = trade.takeProfit;
                    walk.takeProfits ++;
                }
                var profit = sellPrice - buyPrice - COMMISSION;
                trade.profit = profit;
                walk.trades.add(trade);
                if (logToDb) {
                    logTrade(trade);
                }
                trade = null;
            }
            // NiStats.counters.incrementAndGet("totalSteps");
        }
        walk.calculateStats();
        // NiStats.counters.incrementAndGet("totalWalks");
        return walk;
    }

    static List<WalkStep> loadSteps(String split, long checkpoint)  {
        try {
            try (var h = Db.jdbi().open()) {
                var steps = new ArrayList<WalkStep>();
                var dbRes = h.createQuery("""
                    select
                        example.epoch_min,
                        open,
                        close,
                        high,
                        low,
                        predictions
                    from example
                    join example_prediction on example_prediction.epoch_min = example.epoch_min
                    join timeframe on timeframe.epoch_min = example.epoch_min
                    where example_prediction.split = :split
                        and example_prediction.checkpoint = :checkpoint
                        and type = 'm1'
                        and generated = false
                    order by epoch_min
                """)
                    .bind("split", split)
                    .bind("checkpoint", checkpoint)
                    .mapToMap();
                for (var row : dbRes) {
                    var step = new WalkStep();
                    step.epochMin = (Long) row.get("epoch_min");
                    step.high = (Float) row.get("high");
                    step.low = (Float) row.get("low");
                    step.open = (Float) row.get("open");
                    step.close = (Float) row.get("close");
                    var predictions = Db.<Float>dbArrayToList(row.get("predictions"));
                    step.predictions = predictions;
                    steps.add(step);
                }
                return steps;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class SeqStats {
        public DescriptiveStatistics stats = new DescriptiveStatistics();
        public List<Float> seq;

        public SeqStats(List<Float> seq) {
            this.seq = seq;
            for (var f : seq) {
                stats.addValue(f);
            }
        }

        public void print(String name) {
            var subseq = seq;
            if (seq.size() > 5) {
                subseq = seq.subList(0, 5);
            }
            var first5 = subseq.stream()
                    .map(f -> format((double)f))
                    .collect(Collectors.toList());
            System.out.println("SeqStats.print " + name
                    + "\tavg: " + format(stats.getMean())
                    + "\tsum: " + format(stats.getSum())
                    + "\tp0: " + format(stats.getMin())
                    + "\tp10: " + format(stats.getPercentile(10))
                    + "\tp50: " + format(stats.getMean())
                    + "\tp90: " + format(stats.getPercentile(90))
                    + "\tp100: " + format(stats.getMax())
                    + "\tfirst5: " + first5
            );
        }

        public static String format(double f) {
            DecimalFormat df = new DecimalFormat("#");
            df.setMaximumFractionDigits(5);
            return df.format(f);
        }
    }

    static class WalkStep {
        public long epochMin;
        public float high, low, open, close;
        public List<Float> predictions;
    }

    static class WalkTrade {
        public long enterEpochMin, exitEpochMin;
        public float profit, buyPrice, stopLoss, takeProfit;
        public float minPrice = Float.MIN_VALUE;
    }

    static class Walk {
        public long checkpoint;
        public float profit = 0, adjustedProfit, winProfit, lossProfit, enterThreshold, exitThreshold,
                stopLossDelta, takeProfitDelta;
        public int wins = 0, losses = 0, stopLosses = 0, takeProfits = 0, enterBucket, exitBucket;
        public List<WalkTrade> trades = new ArrayList<>();
        public SeqStats profitsStats, lossProfitsStats, winProfitsStats, durationsStats, lossDurationsStats,
                winDurationsStats, dipStats, lossDipStats, winDipStats;
        public long dataGaps;

        public void calculateStats() {
            var winTrades = trades.stream()
                    .filter(t -> t.profit > 0)
                    .collect(Collectors.toList());
            var lossTrades = trades.stream()
                    .filter(t -> t.profit <= 0)
                    .collect(Collectors.toList());
            var profits = trades.stream()
                    .map(t -> t.profit)
                    .collect(Collectors.toList());
            var winProfits = winTrades.stream()
                    .map(t -> t.profit)
                    .collect(Collectors.toList());
            var lossProfits = lossTrades.stream()
                    .map(t -> t.profit)
                    .collect(Collectors.toList());
            var durations = trades.stream()
                    .map(t -> (float)(t.exitEpochMin - t.enterEpochMin))
                    .collect(Collectors.toList());
            var winDurations = winTrades.stream()
                    .map(t -> (float)(t.exitEpochMin - t.enterEpochMin))
                    .collect(Collectors.toList());
            var lossDurations = lossTrades.stream()
                    .map(t -> (float)(t.exitEpochMin - t.enterEpochMin))
                    .collect(Collectors.toList());
            var dips = trades.stream()
                    .map(t -> t.minPrice - t.buyPrice)
                    .collect(Collectors.toList());
            var winDips = winTrades.stream()
                    .map(t -> t.minPrice - t.buyPrice)
                    .collect(Collectors.toList());
            var lossDips = lossTrades.stream()
                    .map(t -> t.minPrice - t.buyPrice)
                    .collect(Collectors.toList());
            this.wins = winTrades.size();
            this.losses = lossTrades.size();
            this.winProfit = (float)winTrades.stream()
                    .mapToDouble(t -> t.profit)
                    .sum();
            this.lossProfit = (float)lossTrades.stream()
                    .mapToDouble(t -> t.profit)
                    .sum();
            this.profit = winProfit + lossProfit;
            this.adjustedProfit = winProfit + 1.5f * lossProfit;
            this.profitsStats = new SeqStats(profits);
            this.lossProfitsStats = new SeqStats(lossProfits);
            this.winProfitsStats = new SeqStats(winProfits);
            this.durationsStats = new SeqStats(durations);
            this.lossDurationsStats = new SeqStats(lossDurations);
            this.winDurationsStats = new SeqStats(winDurations);
            this.dipStats = new SeqStats(dips);
            this.lossDipStats = new SeqStats(lossDips);
            this.winDipStats = new SeqStats(winDips);
        }

        public void print() {
            System.out.println("WalkData.print walk data !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.println("WalkData.print trades = " + trades.size());
            System.out.println("WalkData.print wins = " + wins);
            System.out.println("WalkData.print losses = " + losses);
            System.out.println("WalkData.print profit = " + profit);
            System.out.println("WalkData.print adjusted profit = " + adjustedProfit);
            System.out.println("WalkData.print winProfit = " + winProfit);
            System.out.println("WalkData.print lossProfit = " + lossProfit);
            System.out.println("WalkData.print takeProfits = " + takeProfits);
            System.out.println("WalkData.print stopLosses = " + stopLosses);
            System.out.println("WalkData.print enterBucket = " + enterBucket);
            System.out.println("WalkData.print exitBucket = " + exitBucket);
            System.out.println("WalkData.print enterThreshold = " + enterThreshold);
            System.out.println("WalkData.print exitThreshold = " + exitThreshold);
            System.out.println("WalkData.print stopLossDelta = " + stopLossDelta);
            System.out.println("WalkData.print takeProfitDelta = " + takeProfitDelta);

            this.profitsStats.print("Profits");
            this.lossProfitsStats.print("LossProfits");
            this.winProfitsStats.print("WinProfits");
            this.durationsStats.print("Durations");
            this.lossDurationsStats.print("LossDurations");
            this.winDurationsStats.print("WinDurations");
            this.dipStats.print("Dips");
            this.lossDipStats.print("LossDips");
            this.winDipStats.print("WinDips");
            System.out.println("WalkData.print dataGaps = " + dataGaps);
        }
    }

    private static void initDb() {
        try (var h = Db.jdbi().open()) {
            h.execute("drop table if exists walk_trade");
            h.execute("""
                create table walk_trade (
                    enter_epoch_min bigint,
                    exit_epoch_min bigint,
                    buy_price float,
                    profit float
                )
            """);
        }
    }

    private static void logTrade(WalkTrade trade) {
        try (var h = Db.jdbi().open()) {
            h.createUpdate("""
                insert into walk_trade(enter_epoch_min, exit_epoch_min, buy_price, profit)
                values(:enter_epoch_min, :exit_epoch_min, :buy_price, :profit)
            """)
                    .bind("enter_epoch_min", trade.enterEpochMin)
                    .bind("exit_epoch_min", trade.exitEpochMin)
                    .bind("buy_price", trade.buyPrice)
                    .bind("profit", trade.profit)
                    .execute();
        }
    }
}
