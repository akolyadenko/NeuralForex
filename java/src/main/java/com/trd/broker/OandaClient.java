// ./gradlew -PmainClass=com.trd.broker.OandaClient run
package com.trd.broker;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.trd.util.HttpUtil;
import com.trd.util.NiUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OandaClient {
    private static final String ACCOUNT_ID = "101";

    public static void main(String[] args) throws Exception {
        System.out.println("OandaClient.main active trades: " + NiUtil.toString(getOpenPositions()));
        System.out.println("");
        var tmfs = getLatestTimeframes();
        System.out.println("OandaClient.main submit trade resp: "
                + NiUtil.GSON.toJson(submitTrade(
                        1L,
                        tmfs.m1.lastCandle.bid.c - 0.00185f)));
        System.out.println("");
        System.out.println("OandaClient.main active trades: " + NiUtil.toString(getOpenPositions()));
        System.out.println("OandaClient.main close all positions: " + closeAllPositions());
        System.out.println("OandaClient.main");
        System.out.println("OandaClient.main active trades: " + NiUtil.toString(getOpenPositions()));
    }

    public static String closeAllPositions() {
        System.out.println("OandaClient.closeAllPositions closing all positions: started.");
        var put = """
                {
                    "longUnits": "ALL"
                }
            """;
        var resp = HttpUtil.put(
                transformUrl("https://api-fxtrade.oanda.com/v3/accounts/<ACCOUNT>/positions/EUR_USD/close"),
                put);
        return resp;
    }

    /*
    {
        TradeId: 1990
        "orderCreateTransaction":{
            "id":"1989",
            "accountID":"101-001-22536936-001",
            "userID":22536936,
            "batchID":"1989",
            "requestID":"60986633780055828",
            "time":"2022-06-17T05:44:42.531693051Z",
            "type":"MARKET_ORDER",
            "instrument":"EUR_USD",
            "units":"1",
            "timeInForce":"FOK",
            "positionFill":"DEFAULT",
            "stopLossOnFill":{
                "price":"1.05072",
                "timeInForce":"GTC",
                "triggerMode":"TOP_OF_BOOK"
             },
             "reason":"CLIENT_ORDER"
           },
           "orderFillTransaction":{
             "id":"1990",
             "accountID":"101-001-22536936-001",
             "userID":22536936,
             "batchID":"1989",
             "requestID":"60986633780055828",
             "time":"2022-06-17T05:44:42.531693051Z",
             "type":"ORDER_FILL",
             "orderID":"1989",
             "instrument":"EUR_USD",
             "units":"1",
             "requestedUnits":"1",
             "price":"1.05260",
             "pl":"0.0000",
             "quotePL":"0",
             "financing":"0.0000",
             "baseFinancing":"0",
             "commission":"0.0000",
             "accountBalance":"87617.1943",
             "gainQuoteHomeConversionFactor":"1",
             "lossQuoteHomeConversionFactor":"1",
             "guaranteedExecutionFee":"0.0000",
             "quoteGuaranteedExecutionFee":"0",
             "halfSpreadCost":"0.0001",
             "fullVWAP":"1.05260",
             "reason":"MARKET_ORDER",
             "tradeOpened":{
                "price":"1.05260",
                "tradeID":"1990",
                "units":"1",
                "guaranteedExecutionFee":"0.0000",
                "quoteGuaranteedExecutionFee":"0",
                "halfSpreadCost":"0.0001",
                "initialMarginRequired":"0.0210"},
                "fullPrice":{
                    "closeoutBid":"1.05245",
                    "closeoutAsk":"1.05260",
                    "timestamp":"2022-06-17T05:44:42.311461733Z",
                    "bids":[
                        {
                            "price":"1.05245",
                            "liquidity":"10000000"
                         }
                    ],
                    "asks":[
                        {
                            "price":"1.05260",
                            "liquidity":"10000000"
                        }
                    ]
                 },
                 "homeConversionFactors":{
                    "gainQuoteHome":{
                        "factor":"1"
                    },
                    "lossQuoteHome":{
                        "factor":"1"
                    },
                    "gainBaseHome":{
                        "factor":"1.04725740"
                    },
                    "lossBaseHome":{
                        "factor":"1.05778260"
                    }
                 }
             },
             "relatedTransactionIDs":["1989","1990","1991"],
             "lastTransactionID":"1991"
           } */
    public static class SubmitTradeResponse {
        public OrderFillTransaction orderFillTransaction;
    }

    public static class OrderFillTransaction {
        public String id;
    }

    public static SubmitTradeResponse submitTrade(Long units, Float stopLoss) throws Exception {
        System.out.println("OandaClient.submitTrade submitting trade: started.");
        var post = """
                    {
                              "order": {
                                "units": "%UNITS%",
                                "instrument": "EUR_USD",
                                "timeInForce": "FOK",
                                "type": "MARKET",
                                "positionFill": "DEFAULT",
                                "stopLossOnFill": {
                                    "timeInForce": "GTC",   
                                    "price": "%STOP_LOSS%"
                                }
                              }
                    }
                """;
        post = post.replace("%UNITS%", units.toString());
        post = post.replace("%STOP_LOSS%", NiUtil.formatPrice(stopLoss));
        System.out.println("OandaClient.submitTrade post = " + post);
        var req = transformUrl("https://api-fxtrade.oanda.com/v3/accounts/<ACCOUNT>/orders");
        var resp = HttpUtil.post(req, post);
        System.out.println("OandaClient.submitTrade resp = " + resp);
        return new Gson().fromJson(resp, SubmitTradeResponse.class);
    }

    public static class PositionsResponse {
        public List<Position> positions;
    }

    public static class Position {
        public String instrument;
        @SerializedName(value="long")
        public PositionData longPosition;
    }

    public static class PositionData {
        public List<String> tradeIDs;
    }

    public static PositionsResponse getOpenPositions() {
        String respS = null;
        try {
            var req = "https://api-fxtrade.oanda.com/v3/accounts/<ACCOUNT>/openPositions";
            respS = HttpUtil.get(transformUrl(req));
            return new Gson().fromJson(respS, PositionsResponse.class);
        } catch (Exception e) {
            System.out.println("OandaClient.getOpenPositions exception for response: " + respS);
            throw new RuntimeException(e);
        }
    }

    public static class Timeframes {
        public Timeframe m1, m5, m15, m30;
    }

    public static class Timeframe {
        public List<Candle> candles = new ArrayList<Candle>();
        public Candle lastCandle;
    }

    public static class Candle {
        public boolean complete;
        public long volume;
        public String time;
        public CandleValues ask;
        public CandleValues mid;
        public CandleValues bid;
    }

    public static class CandleValues {
        public float o, h, l, c;
    }

    public static Timeframes getLatestTimeframes() throws Exception {
        var timeframes = new Timeframes();
        timeframes.m1 = getLatestTimeframe("M1");
        timeframes.m5 = getLatestTimeframe("M5");
        timeframes.m15 = getLatestTimeframe("M15");
        timeframes.m30 = getLatestTimeframe("M30");
        return timeframes;
    }

    public static Timeframe getLatestTimeframe(String granularity) throws Exception {
        var respS = HttpUtil.get(
                transformUrl("https://api-fxtrade.oanda.com/v3/instruments/EUR_USD/candles?price=AMB&granularity=" + granularity)
        );
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        var resp = gson.fromJson(respS, Timeframe.class);
        Collections.sort(resp.candles, (c1, c2) -> c1.time.compareTo(c2.time));
        if (!resp.candles.get(resp.candles.size() - 1).complete) {
            resp.candles.remove(resp.candles.size() - 1);
        }
        resp.lastCandle = resp.candles.get(resp.candles.size() - 1);
        // System.out.println("OandaClient.getLatestTimeframe json = " + gson.toJson(resp));
        return resp;
    }

    public static Timeframe getTimeframe(String granularity, long fromEpochSec, long toEpochSec)
            throws Exception {
        var respS = HttpUtil.get(
                transformUrl(
                        "https://api-fxtrade.oanda.com/v3/instruments/EUR_USD/candles?price=AMB&"
                                + "granularity=" + granularity
                                + "&from=" + fromEpochSec
                                + "&to=" + toEpochSec));
        // System.out.println("OandaClient.getTimeframe respS = " + respS);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        var resp = gson.fromJson(respS, Timeframe.class);
        if (resp.candles.size() == 0) {
            return resp;
        }
        Collections.sort(resp.candles, (c1, c2) -> c1.time.compareTo(c2.time));
        if (!resp.candles.get(resp.candles.size() - 1).complete) {
            resp.candles.remove(resp.candles.size() - 1);
        }
        resp.lastCandle = resp.candles.get(resp.candles.size() - 1);
        return resp;
    }

    private static String transformUrl(String req) {
        req = req.replace("<ACCOUNT>", ACCOUNT_ID);
        req = req.replace("api-fxtrade", "api-fxpractice");
        return req;
    }
}
