package com.trd.strategies.exp.eurusd_buy_price_change.opt.io.indicators;

import com.trd.broker.OandaClient;
import com.trd.strategies.exp.eurusd_buy_price_change.opt.io.Timeframe;
import org.ta4j.core.BaseBarSeries;
import org.ta4j.core.BaseBarSeriesBuilder;

import java.time.ZonedDateTime;
import java.util.List;

public class CustomIndicators {
    /*
    public static List<Float> moe(Timeframe.TimeframeCandles cadles, int mins) {
        var series = new BaseBarSeriesBuilder().build();
        var endTime = ZonedDateTime.now();
        for (int i = 0; i < cadles.open.size(); i++) {
            series.addBar(endTime.minusMinutes(mins * (cadles.open.size() - i - 1)),
                    cadles.open.get(i),
                    cadles.high.get(i),
                    cadles.low.get(i),
                    cadles.close.get(i),
                    0);
        }
        var close =
    }
     */
}
