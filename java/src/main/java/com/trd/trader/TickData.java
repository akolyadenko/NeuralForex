package com.trd.trader;

import com.trd.broker.OandaClient;
import com.trd.ml.TfClient;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TickData {
    public Date tickDate;
    public String time;
    public TfClient.TfRequest tfRequest;
    public TfClient.PredictResponse predictions;
    public OandaClient.Timeframes timeframes;
    public OandaClient.PositionsResponse positions;

    public TickData(Date tickDate, TfClient.TfRequest tfRequest,
                    TfClient.PredictResponse predictions, OandaClient.Timeframes timeframes,
                    OandaClient.PositionsResponse positions) {
        this.time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SZ").format(new Date());
        this.tfRequest = tfRequest;
        this.predictions = predictions;
        this.timeframes = timeframes;
        this.positions = positions;
    }
}
