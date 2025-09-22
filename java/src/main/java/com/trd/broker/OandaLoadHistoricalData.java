// ./gradlew -PmainClass=com.trd.broker.OandaLoadHistoricalData run
package com.trd.broker;

import com.google.gson.Gson;
import com.trd.db.Db;
import org.jdbi.v3.core.Handle;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OandaLoadHistoricalData {
    static ExecutorService executorService = Executors.newFixedThreadPool(16);

    public static void main(String[] args) throws Exception {
        try (var h = Db.jdbi().open()) {
            h.execute("drop table if exists oanda_candle");
            h.execute("""
                create table oanda_candle (
                    granularity text,
                    type text,
                    time text,
                    complete boolean,
                    open real,
                    high real,
                    low real,
                    close real,
                    volume real
                )
            """);
        }

        var time = LocalDateTime.now();
        var stopEpochSec = time.toEpochSecond(ZoneOffset.UTC) + 24 * 60 * 60;
        // stopEpochSec += 24 * 60 * 60;
        var epochSec = stopEpochSec - 3 * 365 * 24 * 60 * 60;
        while (epochSec <= stopEpochSec) {
            var toEpochSec = epochSec + 24 * 60 * 60;
            System.out.println("OandaLoadHistoricalData.main epochSec = " + new Date(epochSec * 1000));
            storeCandles("M1", epochSec, toEpochSec);
            // storeCandles("M5", epochSec, toEpochSec);
            // storeCandles("M15", epochSec, toEpochSec);
            // storeCandles("M30", epochSec, toEpochSec);
            epochSec = toEpochSec;
        }
        executorService.shutdown();
        executorService.awaitTermination(1000, TimeUnit.DAYS);
    }

    private static void storeCandles(String granularity, long epochSec, long toEpochSec) throws Exception {
        var tmfs = OandaClient.getTimeframe(granularity, epochSec, toEpochSec);
        System.out.println("OandaLoadHistoricalData.main tmfs candles count = " + tmfs.candles.size());
        executorService.submit(() ->
           storeTimeframe(granularity, tmfs));
        // storeTimeframe(granularity, tmfs);
    }

    private static void storeTimeframe(String granularity, OandaClient.Timeframe tmfs) {
        try {
            if (tmfs.candles.size() > 0) {
                System.out.println("OandaLoadHistoricalData.storeTimeframe processing timeframe with last timestamp: "
                        + tmfs.lastCandle.time);
            }
            try (var h = Db.jdbi().open()) {
                for (var c : tmfs.candles) {
                    storeCandle(h, c, c.bid, granularity, "bid");
                    storeCandle(h, c, c.ask, granularity, "ask");
                    storeCandle(h, c, c.mid, granularity, "mid");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void storeCandle(Handle h, OandaClient.Candle c, OandaClient.CandleValues cv,
                                    String granularity, String type) {
        h.createUpdate("""
            insert into oanda_candle(granularity, type, time, complete, open, high, low, close, volume)
            values(:granularity, :type, :time, :complete, :open, :high, :low, :close, :volume)
        """)
                .bind("granularity", granularity)
                .bind("type", type)
                .bind("time", c.time)
                .bind("complete", c.complete)
                .bind("open", cv.o)
                .bind("high", cv.h)
                .bind("low", cv.l)
                .bind("close", cv.c)
                .bind("volume", c.volume)
                .execute();
    }

}
