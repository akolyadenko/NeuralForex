package com.trd.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.text.DecimalFormat;
import java.util.List;

public class NiUtil {
    public static Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    public static String format(float f) {
        DecimalFormat df = new DecimalFormat("#");
        df.setMaximumFractionDigits(8);
        return df.format(f);
    }

    public static String formatPrice(float f) {
        DecimalFormat df = new DecimalFormat("#");
        df.setMaximumFractionDigits(5);
        return df.format(f);
    }

    public static String toString(Object o) {
        return GSON.toJson(o);
    }

    public static <T> List<T> takeLast(List<T> l, int n) {
        return l.subList(l.size() - n, l.size());
    }
}
