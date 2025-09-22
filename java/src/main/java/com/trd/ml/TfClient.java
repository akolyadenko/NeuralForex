// ./gradlew -PmainClass=com.trd.ml.TfClient run
package com.trd.ml;

import com.beust.jcommander.internal.Lists;
import com.google.gson.Gson;
import com.trd.util.HttpUtil;
import org.apache.http.impl.client.HttpClients;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TfClient {
    public static void main(String[] args) throws Exception {
        try (var http = HttpClients.createDefault()) {
            var req = new TfRequest();
            var instance = new HashMap<String, List<Float>>();
            req.instances.add(instance);
            for (var t : Lists.newArrayList("m1", "m5")) {
                for (var f : Lists.newArrayList("open", "close", "low", "high")) {
                    var feature = t + f + "_rescaled";
                    var l = new ArrayList<Float>();
                    for (int i = 0; i < 60; i ++) {
                        l.add(0f);
                    }
                    instance.put(feature, l);
                }
            }
            var resp = predict(req);
            System.out.println("TfClient.main content = " + resp);
        }
    }

    public static class TfPredictResponse {
        public List<List<Float>> predictions;
    }

    public static class PredictResponse {
        public List<Float> priceDecreasePredictions;
        public List<Float> priceIncreasePredictions;
        public List<Float> pricePredictionsDelta;
    }

    public static PredictResponse predict(TfRequest req) throws Exception {
        var tfResp = HttpUtil.post(
                "http://localhost:8501/v1/models/base:predict",
                new Gson().toJson(req));
        // System.out.println("TfClient.predict tfResp = " + tfResp);
        var tfPredictions = new Gson().fromJson(tfResp, TfPredictResponse.class);
        var resp = new PredictResponse();
        resp.priceDecreasePredictions = tfPredictions.predictions.get(0).subList(0, 100);
        resp.priceIncreasePredictions = tfPredictions.predictions.get(0).subList(100, 200);
        resp.pricePredictionsDelta = new ArrayList<>();
        for (int i = 0; i < 100; i ++) {
            resp.pricePredictionsDelta.add(resp.priceIncreasePredictions.get(i)
                    - resp.priceDecreasePredictions.get(i));
        }
        return resp;
    }

    public static class TfRequest {
        public List<Map<String, List<Float>>> instances = new ArrayList<>();
    }
}
