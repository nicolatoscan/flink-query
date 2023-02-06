package org.apache.flink;
// package com.toscan;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.sink.SinkFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import org.apache.flink.generator.FileDataEntry;
import org.apache.flink.source.SourceFromFile;

import org.apache.flink.metrics.*;
import java.time.LocalDateTime;
import java.time.Instant;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Parallel {

   public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        double scalingFactor = 2;
        int inputRate = 500;
        int numData = 200000;

        final ParameterTool params = ParameterTool.fromArgs(args);
       
    //   String inputFilePath = params.get("input");
        Properties p = new Properties();

        String inputFilePath = "/root/flink-query/test1.csv";

        // data source
        SourceFromFile sourceFromFile = new SourceFromFile(inputFilePath, scalingFactor, inputRate, numData);
        
        // System.out.println("On port: " + 9998);
        DataStream<FileDataEntry> dataStream = env
            .addSource(sourceFromFile, "Source")
			.setParallelism(1)
            .flatMap(new Splitter())
			.setParallelism(1);
            // .keyBy(value -> value.f1)
            // .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            // .sum(1);

        dataStream.addSink(new SinkFunction(p, numData)).name("sink").setParallelism(2);
        dataStream.print();

        env.execute("Ingest and Parse");
   }

   private static final class Splitter implements FlatMapFunction<FileDataEntry, FileDataEntry> {

		//Counter Metrics
		private Counter counter;

		@Override
		public void flatMap(FileDataEntry value, Collector<FileDataEntry> out) throws Exception {
			// normalize and split the line
			String word = value.getPayLoad();

			System.out.println(word);
			if (word.equals("kill")) {
				throw new Exception("Killing the job");
			}

			if (word.charAt(0) != 'a') {
				out.collect(new FileDataEntry(word.toUpperCase(), value.getMsgId(), value.getSourceInTimestamp()));
			} else {
				out.collect(new FileDataEntry("processed", value.getMsgId(),  value.getSourceInTimestamp()));
			}
			// try {
			// 	System.out.println("**************** inc - Counter - Metrics");
			// 	this.counter.inc();
			// } catch (Exception e) {
			// 	e.printStackTrace();
			// }
		}

		// @Override
		// public void open(Configuration config){
		// 	System.out.println("**************** get config - Counter - Metrics");
		// 	this.counter = getRuntimeContext()
        //        .getMetricGroup()
		// 			.addGroup("WordCountMetrics")
        //        .counter("WordCountEventCounter");
		// }

		// @Override
		// public FileDataEntry map(FileDataEntry value) throws Exception {
		// 	// TODO Auto-generated method stub
		// 	return null;
		// }
	}

//    public static class Splitter implements FlatMapFunction<FileDataEntry, Tuple2<String, Integer>>, MetricReporter, Scheduled {

//       private Counter counter;

//       @Override
//       public void flatMap(FileDataEntry sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
//         // System.out.println(sentence.toString());
//          for (String word: sentence.getPayLoad().split(" ")) {
//             // System.out.println("++++" + word.toString());
//             out.collect(new Tuple2<String, Integer>(word, 1));

//             if (word.equals("kill")) {
//                 throw new Exception("Killing the job");
//              }
//             try {
//                report();
//             } catch (Exception e) {
//                e.printStackTrace();
//             }
//          }
//       }

// 		public Long map(String value) throws Exception {
// 			System.out.println("**************** inc - Counter - Metrics");
// 			this.counter.inc();
// 			return this.counter.getCount();
// 		}

//       private static final String lineSeparator = System.lineSeparator();
//       // the initial size roughly fits ~150 metrics with default scope settings
//       private int previousSize = 16384;

//       private final Map<Counter, String> counters = new HashMap<>();
//       private final Map<Gauge<?>, String> gauges = new HashMap<>();
//       private final Map<Histogram, String> histograms = new HashMap<>();
//       private final Map<Meter, String> meters = new HashMap<>();

//       @Override
//       public void open(MetricConfig metricConfig) {
//          System.out.println("**************** get config - Counter - Metrics");
//          report();
//       }

//       @Override
//       public void close() {

//       }

//       @Override
//       public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
//          if (metricName.contains("endExperiment")) {
//                return;
//          }

//         // gets the scope as an array of the scope components, e.g. ["host-7", "taskmanager-2", "window_word_count", "my-mapper"]
//         String[] scopeComponents = metricGroup.getScopeComponents();
//         // gets the identifier of metric, e.g. host-7.taskmanager-2.window_word_count.my-mapper.metricName
// //        String identifier = metricGroup.getMetricIdentifier(metricName);

//         if (scopeComponents.length > 2 && scopeComponents[1].contains("jobmanager")) {
//             return;
//         }

//         StringBuilder sb = new StringBuilder();
//         sb.append(scopeComponents[3]);
//         for (int i = 4; i < scopeComponents.length; i++) {
//             sb.append(".").append(scopeComponents[i]);
//         }
//         sb.append(metricName);

// //        String name = identifier;
//         String name = sb.toString();

//         // save metric to hashMap
//         synchronized (this) {
//             if (metric instanceof Counter) {
//                 counters.put((Counter) metric, name);
//             } else if (metric instanceof Gauge<?>) {
//                 gauges.put((Gauge<?>) metric, name);
//             } else if (metric instanceof Histogram) {
//                 histograms.put((Histogram) metric, name);
//             } else if (metric instanceof Meter) {
//                 meters.put((Meter) metric, name);
//             }
//         }
//     }

//     @Override
//     public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
//         synchronized (this) {
//             if (metric instanceof Counter) {
//                 counters.remove(metric);
//             } else if (metric instanceof Gauge) {
//                 gauges.remove(metric);
//             } else if (metric instanceof Histogram) {
//                 histograms.remove(metric);
//             } else if (metric instanceof Meter) {
//                 meters.remove(metric);
//             }
//         }
//     }

//     // report to the extend system
//     @Override
//     public void report() {

//         try {
//             File mFile = new File("/root/flink-query/metrics_logs/metrics.txt");
//             File eFile = new File("/root/flink-query/metrics_logs/latency_throughput.txt");
//             if (!mFile.exists()) {
//                 mFile.createNewFile();
//             }
//             if (!eFile.exists()) {
//                 eFile.createNewFile();
//             }

//             FileOutputStream mFileOut = new FileOutputStream(mFile, true);
//             FileOutputStream eFileOut = new FileOutputStream(eFile, true);

//             StringBuilder builder = new StringBuilder((int) (this.previousSize * 1.1D));
//             StringBuilder eBuilder = new StringBuilder((int) (this.previousSize * 1.1D));

// //            Instant now = Instant.now();
//             LocalDateTime now = LocalDateTime.now();

//             builder.append(lineSeparator).append(lineSeparator).append(now).append(lineSeparator);
//             eBuilder.append(lineSeparator).append(lineSeparator).append(now).append(lineSeparator);

//             builder.append(lineSeparator).append("---------- Counters ----------").append(lineSeparator);
//             eBuilder.append(lineSeparator).append("---------- records counter ----------").append(lineSeparator);
//             for (Map.Entry metric : counters.entrySet()) {
//                 builder.append(metric.getValue()).append(": ").append(((Counter) metric.getKey()).getCount()).append(lineSeparator);
//                 if (( (String)metric.getValue()).contains("numRecords")) {
//                     eBuilder.append(metric.getValue()).append(": ").append(((Counter) metric.getKey()).getCount()).append(lineSeparator);
//                 }
//             }

//             builder.append(lineSeparator).append("---------- Gauges ----------").append(lineSeparator);
//             for (Map.Entry metric : gauges.entrySet()) {
//                 builder.append(metric.getValue()).append(": ").append(((Gauge) metric.getKey()).getValue()).append(lineSeparator);
//             }

//             builder.append(lineSeparator).append("---------- Meters ----------").append(lineSeparator);
//             eBuilder.append(lineSeparator).append("---------- throughput ----------").append(lineSeparator);
//             for (Map.Entry metric : meters.entrySet()) {
//                 builder.append(metric.getValue()).append(": ").append(((Meter) metric.getKey()).getRate()).append(lineSeparator);
//                 if (((String) metric.getValue()).contains("numRecords")) {
//                     eBuilder.append(metric.getValue()).append(": ").append(((Meter) metric.getKey()).getRate()).append(lineSeparator);
//                 }
//             }

//             builder.append(lineSeparator).append("---------- Histograms ----------").append(lineSeparator);
//             eBuilder.append(lineSeparator).append("---------- lantency ----------").append(lineSeparator);
//             for (Map.Entry metric : histograms.entrySet()) {
//                 HistogramStatistics stats = ((Histogram) metric.getKey()).getStatistics();
//                 builder.append(metric.getValue()).append(": mean=").append(stats.getMean()).append(", min=").append(stats.getMin()).append(", p5=").append(stats.getQuantile(0.05D)).append(", p10=").append(stats.getQuantile(0.1D)).append(", p20=").append(stats.getQuantile(0.2D)).append(", p25=").append(stats.getQuantile(0.25D)).append(", p30=").append(stats.getQuantile(0.3D)).append(", p40=").append(stats.getQuantile(0.4D)).append(", p50=").append(stats.getQuantile(0.5D)).append(", p60=").append(stats.getQuantile(0.6D)).append(", p70=").append(stats.getQuantile(0.7D)).append(", p75=").append(stats.getQuantile(0.75D)).append(", p80=").append(stats.getQuantile(0.8D)).append(", p90=").append(stats.getQuantile(0.9D)).append(", p95=").append(stats.getQuantile(0.95D)).append(", p98=").append(stats.getQuantile(0.98D)).append(", p99=").append(stats.getQuantile(0.99D)).append(", p999=").append(stats.getQuantile(0.999D)).append(", max=").append(stats.getMax()).append(lineSeparator);
//                 eBuilder.append(metric.getValue()).append(": mean=").append(stats.getMean()).append(", min=").append(stats.getMin()).append(", p5=").append(stats.getQuantile(0.05D)).append(", p10=").append(stats.getQuantile(0.1D)).append(", p20=").append(stats.getQuantile(0.2D)).append(", p25=").append(stats.getQuantile(0.25D)).append(", p30=").append(stats.getQuantile(0.3D)).append(", p40=").append(stats.getQuantile(0.4D)).append(", p50=").append(stats.getQuantile(0.5D)).append(", p60=").append(stats.getQuantile(0.6D)).append(", p70=").append(stats.getQuantile(0.7D)).append(", p75=").append(stats.getQuantile(0.75D)).append(", p80=").append(stats.getQuantile(0.8D)).append(", p90=").append(stats.getQuantile(0.9D)).append(", p95=").append(stats.getQuantile(0.95D)).append(", p98=").append(stats.getQuantile(0.98D)).append(", p99=").append(stats.getQuantile(0.99D)).append(", p999=").append(stats.getQuantile(0.999D)).append(", max=").append(stats.getMax()).append(lineSeparator);
//             }

//             mFileOut.write(builder.toString().getBytes());
//             eFileOut.write(eBuilder.toString().getBytes());
//             mFileOut.flush();
//             eFileOut.flush();
//             mFileOut.close();
//             eFileOut.close();
//         } catch (Exception e) {
//             e.printStackTrace();
//         }

//     }
//    }

}