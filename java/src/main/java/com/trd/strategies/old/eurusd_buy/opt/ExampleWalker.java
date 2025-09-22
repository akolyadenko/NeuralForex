// ./gradlew -PmainClass=ExampleWalker run --args="prod"
package com.trd.strategies.old.eurusd_buy.opt;

import com.trd.db.Db;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ExampleWalker {
    public static ExecutorService executorService = Executors.newFixedThreadPool(32);
    public static float SPREAD_COST = 0.0004f;
    public static float SPREAD = 0.0002f;

    public static void main(String[] args) throws Exception {
        initDb();
        System.out.println("ExampleWalker.main starting.");
        var trainSteps = loadSteps("train");
        var testSteps = loadSteps("test");
        var evalSteps = loadSteps("eval");

        findBestWalk(trainSteps, testSteps, evalSteps);

        /*
        System.out.println("ExampleWalker.main custom eval walk: ##############################");
        runWalk(6, 0.60f,
                6, -0.6f, 0.0019f, evalSteps).print();
         */
        executorService.shutdown();
    }

    private static void findBestWalk(List<WalkStep> trainSteps, List<WalkStep> testSteps, List<WalkStep> evalSteps) throws Exception {
        var walks = new LinkedList<Future<Walk>>();
        for (var en = 1; en <= 10; en ++) {
            for (var ex = 1; ex <= 10; ex ++) {
                for (var enterThreshold = 0f; enterThreshold < 1; enterThreshold += 0.05) {
                    for (var exitThreshold = -1f; exitThreshold < 0; exitThreshold += 0.05) {
                        for (var stopLossDelta = 0.0006f; stopLossDelta <= 0.002f; stopLossDelta += 0.0001) {
                            final var walkEn = en;
                            final var walkEnterThreashold = enterThreshold;
                            final var walkEx = ex;
                            final var walkExitThreashold = exitThreshold;
                            final var walkStopLossDelta = stopLossDelta;
                            walks.add(executorService.submit(() -> runWalk(walkEn, walkEnterThreashold, walkEx,
                                    walkExitThreashold, walkStopLossDelta, testSteps, false)));
                        }
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
            for (var en = 1; en <= 20; en++) {
                for (var delta = -0.5f; delta < +0.5f; delta += 0.01) {
                    final var walkEn = en;
                    final var walkDelta = delta;
                    walks.add(executorService.submit(() -> runWalk(
                            walkEn,
                            oldBestWalk.enterThreshold + walkDelta,
                            oldBestWalk.exitBucket,
                            oldBestWalk.exitThreshold,
                            oldBestWalk.stopLossDelta,
                            testSteps,
                            false)));
                }
            }
            bestWalk = findBestWalk(walks, bestWalk);
            final var oldBestWalk2 = bestWalk;
            for (var ex = 1; ex <= 20; ex++) {
                for (var delta = -0.5f; delta < +0.5f; delta += 0.01) {
                    final var walkEx = ex;
                    final var walkDelta = delta;
                    walks.add(executorService.submit(() -> runWalk(
                            oldBestWalk2.enterBucker,
                            oldBestWalk2.enterThreshold,
                            walkEx,
                            oldBestWalk2.exitThreshold + walkDelta,
                            oldBestWalk2.stopLossDelta,
                            testSteps,
                            false)));
                }
            }
            bestWalk = findBestWalk(walks, bestWalk);
            final var oldBestWalk3 = bestWalk;
            for (var delta = -0.001f; delta < +0.001f && oldBestWalk3.stopLossDelta + delta < 0.002f;
                 delta += 0.0001) {
                final var walkDelta = delta;
                walks.add(executorService.submit(() -> runWalk(
                        oldBestWalk3.enterBucker,
                        oldBestWalk3.enterThreshold,
                        oldBestWalk3.exitBucket,
                        oldBestWalk3.exitThreshold,
                        oldBestWalk3.stopLossDelta + walkDelta,
                        testSteps, false)));
            }
            bestWalk = findBestWalk(walks, bestWalk);
        } while(bestWalk.adjustedProfit > prevBestWalk.adjustedProfit);
        System.out.println("ExampleWalker.main train walk: ##############################");
        System.out.println("ExampleWalker.main steps: " + trainSteps.size());
        runWalk(bestWalk.enterBucker, bestWalk.enterThreshold,
                bestWalk.exitBucket, bestWalk.exitThreshold, bestWalk.stopLossDelta, trainSteps, false)
                .print();
        System.out.println("ExampleWalker.main eval walk: ##############################");
        System.out.println("ExampleWalker.main steps: " + evalSteps.size());
        runWalk(bestWalk.enterBucker, bestWalk.enterThreshold,
                bestWalk.exitBucket, bestWalk.exitThreshold, bestWalk.stopLossDelta, evalSteps, false)
                .print();
    }

    private static Walk findBestWalk(LinkedList<Future<Walk>> walks, Walk bestWalk) throws Exception {
        while (!walks.isEmpty()) {
            var walk = walks.poll().get();
            if (
                    walk.adjustedProfit > bestWalk.adjustedProfit
                            // Math.abs(walk.winProfit / walk.lossProfit)
                            //        > Math.abs(bestWalk.winProfit / bestWalk.lossProfit)
                            // && walk.winProfitsStats.stats.getPercentile(10) > 0.0001
                            && walk.wins > walk.losses * 2
                            && walk.trades.size() > 120
            ) {
                bestWalk = walk;
                bestWalk.print();
            }
        }
        return bestWalk;
    }

    private static Walk runWalk(int enterBucket, float enterThreshold, int exitBucket,
                                float exitThreshold, float stopLossDelta, List<WalkStep> steps,
                                boolean logToDb) throws SQLException {
        var walk = new Walk();
        walk.enterBucker = enterBucket;
        walk.enterThreshold = enterThreshold;
        walk.exitBucket = exitBucket;
        walk.exitThreshold = exitThreshold;
        walk.stopLossDelta = stopLossDelta;
        boolean inTrade = false;
        float buyPrice = 0;
        WalkTrade trade = null;
        for (var step : steps) {
            if (trade != null) {
                trade.minPrice = Math.min(trade.minPrice, step.low - SPREAD);
            }
            if (!inTrade && step.pricePredictionsDelta.get(enterBucket) > enterThreshold) {
                inTrade = true;
                buyPrice = step.open;
                trade = new WalkTrade();
                trade.buyPrice = buyPrice;
                // trade.stopLoss = buyPrice - 0.0015f;
                trade.stopLoss = buyPrice - stopLossDelta;
                trade.minPrice = step.low - SPREAD;
                trade.enterEpochMin = step.epochMin;
            } else if (inTrade && (
                    step.pricePredictionsDelta.get(exitBucket) < exitThreshold
                            || step.low - SPREAD <= trade.stopLoss
                            || step.epochMin - trade.enterEpochMin > 120
            )) {
                inTrade = false;
                if (step.epochMin - trade.enterEpochMin > 1400) { // One day.
                    trade = null;
                    continue;
                }
                trade.exitEpochMin = step.epochMin;
                var sellPrice = step.open - SPREAD_COST;
                if (step.low - SPREAD <= trade.stopLoss) {
                    sellPrice = trade.stopLoss + SPREAD - SPREAD_COST;
                    walk.stopLosses ++;
                }
                var profit = sellPrice - buyPrice;
                trade.profit = profit;
                walk.trades.add(trade);
                if (logToDb) {
                    logTrade(trade);
                }
                trade = null;
            }
        }
        walk.calculateStats();
        return walk;
    }

    static List<WalkStep> loadSteps(String split) throws Exception {
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
                    and type = 'm1'
                order by epoch_min
            """)
                    .bind("split", split)
                    .mapToMap();
            for (var row : dbRes) {
                var step = new WalkStep();
                step.epochMin = (Long) row.get("epoch_min");
                step.high = (Float) row.get("high");
                step.low = (Float) row.get("low");
                step.open = (Float) row.get("open");
                step.close = (Float) row.get("close");
                var predictions = Db.<Float>dbArrayToList(row.get("predictions"));
                step.priceDecreasePredictions = predictions.subList(0, 100);
                step.priceIncreasePredictions = predictions.subList(100, 200);
                for (int i = 0; i < 100; i ++) {
                    step.pricePredictionsDelta.add(step.priceIncreasePredictions.get(i)
                            - step.priceDecreasePredictions.get(i));
                }
                steps.add(step);
            }
            return steps;
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
                    + "\tsum: " + format(stats.getSum())
                    + "\tavg: " + format(stats.getMean())
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
        public List<Float> priceIncreasePredictions = new ArrayList<>();
        public List<Float> priceDecreasePredictions = new ArrayList<>();
        public List<Float> pricePredictionsDelta = new ArrayList<>();
    }

    static class WalkTrade {
        public long enterEpochMin, exitEpochMin;
        public float profit, buyPrice, stopLoss;
        public float minPrice = Float.MIN_VALUE;
    }

    static class Walk {
        public float profit = 0, adjustedProfit, winProfit, lossProfit, enterThreshold, exitThreshold, stopLossDelta;
        public int wins = 0, losses = 0, stopLosses = 0, enterBucker, exitBucket;
        public List<WalkTrade> trades = new ArrayList<>();
        public SeqStats profitsStats, lossProfitsStats, winProfitsStats, durationsStats, lossDurationsStats,
                winDurationsStats, dipStats, lossDipStats, winDipStats;

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
            this.adjustedProfit = winProfit + 3 * lossProfit;
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
            System.out.println("WalkData.print stopLosses = " + stopLosses);
            System.out.println("WalkData.print enterBucket = " + enterBucker);
            System.out.println("WalkData.print exitBucket = " + exitBucket);
            System.out.println("WalkData.print enterThreshold = " + enterThreshold);
            System.out.println("WalkData.print exitThreshold = " + exitThreshold);
            System.out.println("WalkData.print stopLossDelta = " + stopLossDelta);

            this.profitsStats.print("Profits");
            this.lossProfitsStats.print("LossProfits");
            this.winProfitsStats.print("WinProfits");
            this.durationsStats.print("Durations");
            this.lossDurationsStats.print("LossDurations");
            this.winDurationsStats.print("WinDurations");
            this.dipStats.print("Dips");
            this.lossDipStats.print("LossDips");
            this.winDipStats.print("WinDips");
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
