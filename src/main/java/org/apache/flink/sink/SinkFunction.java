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
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class SinkFunction extends RichSinkFunction<FileDataEntry> {
    private static Logger l = LoggerFactory.getLogger(SinkFunction.class);

    private Properties p;

    private FileWriter writer;

    // metric
    private Counter sinkCounter;
    private Meter sinkMeter;
    private Gauge endExpGauge;
    private Gauge<Double> cpuGauge;
    private Histogram latencyHistogram;
    // private Runtime runtime;


    // parameters
    private int dataNum;
    private String flag;
    private String nameComponent;
    private Boolean workerFail;

    private Boolean isExperimentEnding = false;
    private Double cpuCurValue = 0.0;
    private static final long MEGABYTE = 1024L * 1024L;

    // private transient OperatingSystemMXBean osBean;

    // private MQTTPublishTask mqttPublishTask;

    public SinkFunction(Properties p_, int dataNum, String flag, String nameComponent, Boolean workerFail) {
        p = p_;
        this.dataNum = dataNum;
        this.flag = flag;
        this.workerFail = workerFail;
        this.nameComponent = nameComponent;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // osBean = ManagementFactory.getOperatingSystemMXBean();

        Random rand = new Random();

        int int_random = rand.nextInt(500); 

        if (this.workerFail) {
            writer = new FileWriter("/root/flink-query/metrics_logs/" + this.nameComponent + "_" + this.flag + "_metrics_FT_" + int_random + ".csv");
        } else {
            writer = new FileWriter("/root/flink-query/metrics_logs/" + this.nameComponent + "_" + this.flag + "_metrics_NR_" + int_random + ".csv");
        }

        // mqttPublishTask = new MQTTPublishTask();
        // mqttPublishTask.setup(l, p);

        sinkCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("MyMetrics")
                .counter("SinkCounter");
        sinkMeter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("MyMetrics")
                    .meter("SinkMeter", new MeterView(10));
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
        
        Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
        String identifier = vars.get("<host>") + ".taskmanager." + vars.get("<tm_id>") + "." + vars.get("<job_name>") + "." + vars.get("<task_name>") + "." + vars.get("<subtask_index>");        
        cpuGauge = getRuntimeContext().getMetricGroup()
                .gauge(getRuntimeContext().getMetricGroup().getMetricIdentifier(identifier+".Status.JVM.CPU"+".Load"), new Gauge<Double>(){
                    @Override
                    public Double getValue() {
                        return cpuCurValue;
                    }
                });
        System.out.println(cpuGauge.getValue());

        System.out.println(getRuntimeContext().getHistogram(getRuntimeContext().getMetricGroup().getMetricIdentifier(identifier+".Status.JVM.CPU"+".Load")));

        // System.out.println(getRuntimeContext().getMetricGroup().getIOMetricGroup().);

        // runtime = Runtime.getRuntime();
    }

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
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
    
        // runtime.gc();
        // // Calculate the used memory
        // long memory = bytesToMegabytes(runtime.totalMemory() - runtime.freeMemory());

        if (value.getSourceInTimestamp() > 0) {
            latencyHistogram.update(Instant.now().toEpochMilli() - value.getSourceInTimestamp());
            try {
                
                writer.write(Instant.now().toEpochMilli() + "," + value.getMsgId() + "," + value.getPayLoad() + "," + value.getSourceInTimestamp() + "," + sinkCounter.getCount() + "," + sinkMeter.getRate());
                // + "," + latencyHistogram.getStatistics().getMax() + ","  + latencyHistogram.getStatistics().getMean() + ","  + latencyHistogram.getStatistics().getMin() + ","  + latencyHistogram.getStatistics().getStdDev() + ","  + (Instant.now().toEpochMilli() - value.getSourceInTimestamp()) + "," + endExpGauge.getValue());
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
