package org.apache.flink.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.generator.*;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Properties;

public class SinkFunction extends RichSinkFunction<FileDataEntry> {
    private static Logger l = LoggerFactory.getLogger(SinkFunction.class);

    private Properties p;

    private FileWriter writer;

    // metric
    private Counter sinkCounter;
    private Meter sinkMeter;
    private Gauge endExpGauge;
    private Histogram latencyHistogram;
    private int dataNum;
    private Boolean isExperimentEnding = false;

    // private MQTTPublishTask mqttPublishTask;

    public SinkFunction(Properties p_, int dataNum) {
        p = p_;
        this.dataNum = dataNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        writer = new FileWriter("/root/flink-query/metrics_logs/metrics.csv");

        // mqttPublishTask = new MQTTPublishTask();
        // mqttPublishTask.setup(l, p);

        sinkCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("MyMetrics")
                .counter("SinkCounter");
        sinkMeter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("MyMetrics")
                    .meter("SinkMeter", new MeterView(1));
        com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(new SlidingWindowReservoir(dataNum));
        latencyHistogram = getRuntimeContext().getMetricGroup()
                .addGroup("MyMetrics")
                .histogram("latency", new DropwizardHistogramWrapper(dropwizardHistogram));
        endExpGauge = getRuntimeContext().getMetricGroup()
                .addGroup("MyMetrics")
                .gauge("endExperiment", new Gauge<Boolean>() {
                    @Override
                    public Boolean getValue() {
                        return isExperimentEnding;
                    }
                });
    }

    @Override
    public void close() throws Exception {
        super.close();
        // mqttPublishTask.tearDown();
        writer.close();
    }

    @Override
    public void invoke(FileDataEntry value, Context context) throws Exception {
        HashMap<String, String> map = new HashMap<>();
        // map.put(AbstractTask.DEFAULT_KEY, value.getObsValue());
        // mqttPublishTask.doTask(map);

        sinkCounter.inc();
        sinkMeter.markEvent();
        
        if (value.getSourceInTimestamp() > 0) {
            latencyHistogram.update(Instant.now().toEpochMilli() - value.getSourceInTimestamp());
            try {
                writer.write(value.getMsgId() + "," + value.getPayLoad() + "," + "," + value.getSourceInTimestamp() + "," + (Instant.now().toEpochMilli() - value.getSourceInTimestamp()));
                writer.write("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (value.getSourceInTimestamp() < -1) {
            isExperimentEnding = true;
        }
    }
}
