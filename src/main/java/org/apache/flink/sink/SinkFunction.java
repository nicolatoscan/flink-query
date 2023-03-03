package org.apache.flink.sink;

import com.codahale.metrics.SlidingWindowReservoir;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.generator.*;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SinkFunction extends RichSinkFunction<FileDataEntry> {
    private static Logger l = LoggerFactory.getLogger(SinkFunction.class);

    private Properties p;

    private FileWriter writer;

    // metric
    private Counter sinkCounter;
    private Counter numBytesInCounter;
    private Counter numBytesOutCounter;
    private Counter numRecordsInCounter;
    private Counter numRecordsOutCounter;
    private Meter sinkMeter;
    private Meter numBytesMeter;
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

        // /mnt/d/Knowledge_Base/flink-query
        if (this.workerFail) {
            writer = new FileWriter("/mnt/d/Knowledge_Base/flink-query/metrics_logs/" + this.nameComponent + "_" + this.flag + "_metrics_FT_" + int_random + ".csv");
        } else {
            writer = new FileWriter("/mnt/d/Knowledge_Base/flink-query/metrics_logs/" + this.nameComponent + "_" + this.flag + "_metrics_NR_" + int_random + ".csv");
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

        // System.out.println(getRuntimeContext().getHistogram(getRuntimeContext().getMetricGroup().getMetricIdentifier(identifier+".Status.JVM.CPU"+".Load")));

        // System.out.println(getRuntimeContext().getMetricGroup().getIOMetricGroup().);

        // runtime = Runtime.getRuntime();
        // numBytesInCounter = getRuntimeContext().getMetricGroup().counter(identifier+".Shuffle.Netty.Input.numBytesInLocal");
        // numBytesOutCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumBytesOutCounter();
        // numRecordsInCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        // numRecordsOutCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

        // numBytesMeter = getRuntimeContext().getMetricGroup().meter(identifier+".Shuffle.Netty.Input.numBytesInRemotePerSecond", new MeterView(10));
        // System.out.println(identifier+".Shuffle.Netty.Input.numBytesInRemotePerSecond");
        // <host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>
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
        long now = context.currentProcessingTime();

        sinkCounter.inc();
        // numBytesInCounter.inc();
        // numBytesOutCounter.inc();
        // numRecordsInCounter.inc();
        // numRecordsOutCounter.inc();
        sinkMeter.markEvent();

        // numBytesMeter.markEvent();
    
        // System.out.println();
        // runtime.gc();
        // // Calculate the used memory
        // long memory = bytesToMegabytes(runtime.totalMemory() - runtime.freeMemory());

        if (value.getSourceInTimestamp() > 0) {
            // latencyHistogram.update(Instant.now().toEpochMilli() - value.getSourceInTimestamp());
            try {
                
                writer.write((now - value.getSourceInTimestamp()) + "," + value.getMsgId() + "," + value.getSourceInTimestamp() + "," + sinkCounter.getCount() + "," + sinkMeter.getRate() + "," + value.getLength()
                //  + "," + latencyHistogram.getStatistics().getMax() + ","  + latencyHistogram.getStatistics().getMean() + ","  + latencyHistogram.getStatistics().getMin() + ","  + latencyHistogram.getStatistics().getStdDev() + ","  + () + "," + endExpGauge.getValue()
                );
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
