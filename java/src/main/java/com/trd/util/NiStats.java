package com.trd.util;

import com.google.common.util.concurrent.AtomicLongMap;

import java.text.DecimalFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NiStats {
    public static AtomicLongMap<String> counters = AtomicLongMap.create();
    private static DecimalFormat LONG_FORMATTER = new DecimalFormat("#,###");
    private static ScheduledExecutorService scheduler;

    public static void printEvery5Second() {
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {print();}, 5, 5, TimeUnit.SECONDS);
    }

    public static void print() {
        System.out.println("NiStats.print ------ counters start --------");
        counters.asMap().entrySet().stream()
                .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                .forEach(e -> System.out.println("NiStats.print " + e.getKey() + " = \t"
                        // + formatShort(e.getValue()) + " : "
                        + format(e.getValue())));
        System.out.println("NiStats.print ------ counters end --------");
    }

    public static void stopAndPrint() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        print();
    }

    static String format(long l) {
        return LONG_FORMATTER.format(l);
    }

    static String formatShort(long l) {
        if (l > 1000L * 1000 * 1000 * 1000) {
            return l / (1000 * 1000 * 1000 * 1000) + "T";
        }
        if (l > 1000L * 1000 * 1000) {
            return l / (1000 * 1000 * 1000) + "G";
        }
        if (l > 1000L * 1000) {
            return l / (1000 * 1000) + "M";
        }
        if (l > 1000L) {
            return l / 1000L + "K";
        }
        return l + "";
    }
}
