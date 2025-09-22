// ./gradlew -PmainClass=com.trd.trader.ExpandTickData run
package com.trd.trader;

import com.google.gson.Gson;
import com.trd.db.Db;

import java.sql.Types;
import java.util.stream.Collectors;

public class ExpandTickData {
    public static void main(String[] args) throws Exception {
         try (var h = Db.jdbi().open()) {
             h.execute("drop table if exists trader_tick_data_expanded");
             h.execute("""
                create table trader_tick_data_expanded (
                    time text,
                    m1close_rescaled real[],
                    m1close real[],
                    m5close real[],
                    m1time text[],
                    m5time text[],
                    price_decrease_predictions real[],
                    price_increase_predictions real[]
                )
             """);
             h.execute("drop table if exists trader_tick_data_expanded_features");
             h.execute("""
                create table trader_tick_data_expanded_features (
                    time text,
                    type text,
                    data real[]
                )
             """);
             var dbRes = h.createQuery("""
                select data::text from trader_tick_data
             """).mapToMap();
             for (var row : dbRes) {
                 var tickData = new Gson().fromJson((String)row.get("data"), TickData.class);
                 var time = tickData.time;
                 var m1closeRescaled = tickData.tfRequest.instances.get(0).get("m1close_rescaled");
                 var m1close = tickData.timeframes.m1.candles.stream()
                         .map(c -> c.bid.c).collect(Collectors.toList());
                 var m5close = tickData.timeframes.m5.candles.stream()
                         .map(c -> c.bid.c).collect(Collectors.toList());
                 var priceDecreasePredictions = tickData.predictions.priceDecreasePredictions;
                 var priceIncreasePredictions = tickData.predictions.priceIncreasePredictions;
                 h.createUpdate("""
                    insert into trader_tick_data_expanded(time, m1close_rescaled, m1close, m5close,
                            m1time, m5time,
                            price_decrease_predictions, price_increase_predictions) 
                        values(:time, :m1CloseRescaled, :m1Close, :m5Close,
                            :m1Time, :m5Time, 
                            :priceDecreasePredictions, :priceIncreasePredictions)
                 """)
                         .bind("time", time)
                         .bindBySqlType("m1CloseRescaled",
                                 Db.createFloatArray(h, m1closeRescaled), Types.ARRAY)
                         .bindBySqlType("m1Close",
                                 Db.createFloatArray(h, m1close), Types.ARRAY)
                         .bindBySqlType("m5Close",
                                 Db.createFloatArray(h, m5close), Types.ARRAY)
                         .bindBySqlType("m1Time",
                                 Db.createStringArray(h,
                                         tickData.timeframes.m1.candles.stream().map(c -> c.time)
                                                 .collect(Collectors.toList())),
                                 Types.ARRAY)
                         .bindBySqlType("m5Time",
                                 Db.createStringArray(h,
                                         tickData.timeframes.m5.candles.stream().map(c -> c.time)
                                                 .collect(Collectors.toList())),
                                 Types.ARRAY)
                         .bindBySqlType("priceDecreasePredictions",
                                 Db.createFloatArray(h, priceDecreasePredictions), Types.ARRAY)
                         .bindBySqlType("priceIncreasePredictions",
                                 Db.createFloatArray(h, priceIncreasePredictions), Types.ARRAY)
                         .execute();
                 for (var feature : tickData.tfRequest.instances.get(0).keySet()) {
                     h.createUpdate("""
                        insert into trader_tick_data_expanded_features (time, type, data)
                        values (:time, :type, :data)
                     """)
                             .bind("time", time)
                             .bind("type", feature)
                             .bindBySqlType(
                                     "data",
                                        Db.createFloatArray(h,
                                                tickData.tfRequest.instances.get(0).get(feature)), Types.ARRAY)
                             .execute();
                 }
             }
         }
    }
}
