// ./gradlew -PmainClass=com.trd.trader.TraderMain run --args="prod"
package com.trd.trader;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.trd.broker.OandaClient;
import com.trd.db.Db;
import com.trd.ml.TfClient;
import com.trd.strategies.TradingStrategy;
import com.trd.strategies.prod.eurusd_buy_spread.TradingStrategyImpl;
import com.trd.util.HttpUtil;
import com.trd.util.NiUtil;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TraderMain {
    public static final long TRADE_UNITS = 3 * 1000 * 1000;
    public static List<TradingStrategy> strategies = Lists.newArrayList(
            new TradingStrategyImpl()
    );

    public static void main(String[] args) throws Exception {
        HttpUtil.disableHttpLogging();
        var prev1PredictionDelta = 0f;
        var failures = 0;
        while (true) {
            try {
                var timer = Stopwatch.createStarted();
                var tmfs = OandaClient.getLatestTimeframes();
                assert tmfs.m1.candles.size() == 60;
                var tfReq = convertTimeframesToTfRequest(tmfs);
                var predictions = TfClient.predict(tfReq);
                var positions = OandaClient.getOpenPositions();
                var tickData = new TickData(new Date(), tfReq, predictions, tmfs, positions);
                if (prev1PredictionDelta != predictions.pricePredictionsDelta.get(1)) {
                    System.out.println("\nTraderMain.main ================= Tick: " + tickData.time
                            + " =====================");
                    System.out.println("TraderMain.main last bid price: " + tickData.timeframes.m1.lastCandle.bid.c);
                    var m1bids = tmfs.m1.candles.stream().map(c -> c.bid.c)
                            .collect(Collectors.toList());
                    var m1times = tmfs.m1.candles.stream().map(c -> c.time)
                            .collect(Collectors.toList());
                    System.out.println("TraderMain.main m1bids size = " + m1bids.size());
                    System.out.println("TraderMain.main m1 bid = " + m1bids.subList(m1bids.size() - 10, m1bids.size()));
                    System.out.println("TraderMain.main m1 time = " + m1times.subList(m1times.size() - 10, m1times.size()));
                    System.out.println("TraderMain.main priceDecreasePredictions = " + predictions.priceDecreasePredictions);
                    System.out.println("TraderMain.main priceIncreasePredictions = " + predictions.priceIncreasePredictions);
                    System.out.println("TraderMain.main pricePredictionsDelta = " + predictions.pricePredictionsDelta);
                    System.out.println("TraderMain.main pricePredictionsDelta bucket 5 = "
                            + predictions.pricePredictionsDelta.get(5));
                    System.out.println("TraderMain.main pricePredictionsDelta bucket 10 = "
                            + predictions.pricePredictionsDelta.get(10));
                    System.out.println("TraderMain.main active trades = " + NiUtil.toString(positions));
                    prev1PredictionDelta = predictions.pricePredictionsDelta.get(1);
                    for (var strategy : strategies) {
                        strategy.processTick(tickData);
                    }
                    logTick(tickData);
                    System.out.print("Tradeloop time: ");
                }
                System.out.print(timer.stop().elapsed(TimeUnit.SECONDS) + "s. ");
                Thread.sleep(1000);
                failures = 0;
            } catch (Throwable e) {
                e.printStackTrace();
                failures += 1;
                if (failures == 300) {
                    System.out.println("TraderMain.main 20 failures in row. Closing all positions and exiting.");
                    OandaClient.closeAllPositions();
                    break;
                }
            }
        }
    }

    static TfClient.TfRequest convertTimeframesToTfRequest(OandaClient.Timeframes tfms) {
        var req = new TfClient.TfRequest();
        var m1 = convertTimeframeToFeatures(tfms.m1, "m1");
        var m5 = convertTimeframeToFeatures(tfms.m5, "m5");
        var m15 = convertTimeframeToFeatures(tfms.m15, "m15");
        var m30 = convertTimeframeToFeatures(tfms.m30, "m30");
        m1.putAll(m5);
        m1.putAll(m15);
        m1.putAll(m30);
        var basePrice = m1.get("m1close").get(59);
        for (var t : Lists.newArrayList("m1", "m5", "m15", "m30")) {
            for (var f : Lists.newArrayList("open", "high", "low", "close")) {
                var feature = t + f;
                m1.put(feature + "_rescaled", rescale(m1.get(feature), basePrice));
                m1.remove(feature);
            }
        }

        req.instances.add(m1);
        return req;
    }

    static List<Float> rescale(List<Float> l, float base) {
        return l.stream()
                .map( v -> v - base)
                .collect(Collectors.toList());
    }

    static Map<String, List<Float>> convertTimeframeToFeatures(OandaClient.Timeframe tfm, String prefix) {
        var map = new HashMap<String, List<Float>>();
        var open = new ArrayList<Float>();
        var high = new ArrayList<Float>();
        var low = new ArrayList<Float>();
        var close = new ArrayList<Float>();
        var candles = Lists.newArrayList(tfm.candles);
        var lastCandle = candles.get(candles.size() - 1);
        if (!lastCandle.complete) {
            candles.remove(candles.size() - 1);
        }
        candles = candles.subList(candles.size() - 60, candles.size());
        for (var c : candles) {
            assert c.complete;
            open.add(c.bid.o);
            high.add(c.bid.h);
            low.add(c.bid.l);
            close.add(c.bid.c);
        }
        map.put(prefix + "open", open);
        map.put(prefix + "high", high);
        map.put(prefix + "low", low);
        map.put(prefix + "close", close);
        return map;
    }

    static void logTick(TickData tickData) {
        try (var h = Db.jdbi().open()) {
            var m1Close =

            h.createUpdate("""
                        insert into trader_tick_data values(to_json(:data::json))
                    """)
                    .bind("data", new Gson().toJson(tickData))
                    .execute();
        }
    }
}
