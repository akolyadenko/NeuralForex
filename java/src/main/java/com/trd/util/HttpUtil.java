package com.trd.util;

import com.beust.jcommander.Strings;
import com.beust.jcommander.internal.Lists;
import com.google.gson.Gson;
import com.trd.ml.TfClient;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

public class HttpUtil {
    static {
        disableHttpLogging();
    }

    static CloseableHttpClient http = HttpClients.createDefault();

    public static String get(String url) throws Exception {
        var get = new HttpGet(url);
        get.addHeader("Authorization",
                    "Bearer  87a217a5cff3456dbb7f6bd91e7c2e41-c802202802f7382112983cc9300776e9");
        try (var resp = http.execute(get)) {
            var respOut = resp.getEntity().getContent();
            var content = IOUtils.readLines(respOut, Charset.defaultCharset());
            return Strings.join("\n", content);
        }
    }

    public static String post(String url, String payload) throws Exception {
        var post = new HttpPost(url);
        post.addHeader("Content-Type", "application/json");
        post.addHeader("Authorization",
                    "Bearer  87a217a5cff3456dbb7f6bd91e7c2e41-c802202802f7382112983cc9300776e9");
        var sreq = new StringEntity(payload, "UTF-8");
        post.setEntity(sreq);
        try (var resp = http.execute(post)) {
            var respOut = resp.getEntity().getContent();
            var content = IOUtils.readLines(respOut, Charset.defaultCharset());
            return Strings.join("\n", content);
        }
    }

    public static String put(String url, String payload) {
        try {
            var post = new HttpPut(url);
            post.addHeader("Content-Type", "application/json");
            post.addHeader("Authorization",
                        "Bearer  87a217a5cff3456dbb7f6bd91e7c2e41-c802202802f7382112983cc9300776e9");
            var sreq = new StringEntity(payload, "UTF-8");
            post.setEntity(sreq);
            try (var resp = http.execute(post)) {
                var respOut = resp.getEntity().getContent();
                var content = IOUtils.readLines(respOut, Charset.defaultCharset());
                return Strings.join("\n", content);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void disableHttpLogging() {
        java.util.logging.Logger.getLogger("org.apache.http.wire").setLevel(Level.OFF);
        java.util.logging.Logger.getLogger("org.apache.http.headers").setLevel(java.util.logging.Level.OFF);
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
        System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
        System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire", "ERROR");
        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "ERROR");
        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.headers", "ERROR");
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
                org.slf4j.LoggerFactory.getLogger("org.apache.http");
        root.setLevel(ch.qos.logback.classic.Level.OFF);
    }
}
