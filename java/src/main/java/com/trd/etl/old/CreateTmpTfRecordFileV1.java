// ./gradlew -PmainClass=com.trd.etl.exp.CreateTmpTfRecordFile run --args="prod"
package com.trd.etl.old;

import com.trd.util.TfUtil;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.FloatList;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Random;

public class CreateTmpTfRecordFileV1 {
    public static void main(String[] args) throws Exception {
        DataOutputStream out = new DataOutputStream(new FileOutputStream("/usr/proj/trd/java_test.tfrecord"));
        for (int k = 0; k < 100000; k++) {
            var e = Example.newBuilder();
            var features = e.getFeaturesBuilder();
            var m1open = new ArrayList<Float>();
            for (int i = 0; i < 60; i++) {
                m1open.add(new Random().nextInt(10) + 0f);
            }
            features.putFeature("m1open",
                    Feature.newBuilder().setFloatList(
                            FloatList.newBuilder().addAllValue(m1open)).build());
            var histogram = new ArrayList<Float>();
            histogram.add(1f);
            for (int i = 0; i < 99; i++) {
                histogram.add(new Random().nextInt(1) + 0f);
            }
            features.putFeature("profit_loss_histogram",
                    Feature.newBuilder().setFloatList(
                            FloatList.newBuilder().addAllValue(histogram)).build());
            TfUtil.write(out, e.build().toByteArray());
        }
        out.close();
    }
}
