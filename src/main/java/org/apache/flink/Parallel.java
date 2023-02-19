package org.apache.flink;
// package com.toscan;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.generator.FileDataEntry;
import org.apache.flink.metrics.*;
import org.apache.flink.sink.SinkFunction;
import org.apache.flink.source.SourceFromFile;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



public class Parallel {

   public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
		// env.setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        double scalingFactor = 2;
        int inputRate = 500;
        int numData = 9000000;

		// String server = "localhost:9092";
		// String inputTopic = "testtopic";

        final ParameterTool params = ParameterTool.fromArgs(args);
       
    //   String inputFilePath = params.get("input");
        Properties p = new Properties();

        String inputFilePath = "./test1.csv";

		TimeUnit.MILLISECONDS.sleep(30000);

        // data source
        SourceFromFile sourceFromFile = new SourceFromFile(inputFilePath, scalingFactor, inputRate, numData);

		// kafka source
		// KafkaSource<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);

		// DataStream<FileDataEntry> dataStream = env.fromSource(flinkKafkaConsumer, WatermarkStrategy.noWatermarks(), "Kafka Source")
		// .flatMap(new Splitter());

        // System.out.println("On port: " + 9998);
        DataStream<FileDataEntry> dataStream = env
            .addSource(sourceFromFile, "Source");

		DataStream<FileDataEntry> flatmappedStream = dataStream
            .flatMap(new Splitter())
			.setParallelism(2);
            // .keyBy(value -> value.f1)
            // .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            // .sum(1);

		DataStream<FileDataEntry> mappedStream = flatmappedStream
		.map(new Splitter_Revers())
		.setParallelism(2);

		// DataStream<FileDataEntry> joinedStream = flatmappedStream
		// .join(mappedStream);

		System.out.println("Start!");
        mappedStream.addSink(new SinkFunction(p, numData, "EO", "Sink_2", true)).name("sink").setParallelism(1);
        // dataStream.print();
		
		env.execute("parallel");
   }

	// public static KafkaSource<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
	// 	Properties props = new Properties();
	// 	props.setProperty("bootstrap.servers", kafkaAddress);
	// 	props.setProperty("enable.auto.commit", "false");
	// 	props.setProperty("auto.commit.interval.ms", "1000");
	// 	props.setProperty("auto.offset.reset", "earlist");

	// 	KafkaSource<String> consumer = KafkaSource
	// 	.<String>builder()
	// 	.setProperties(props)
	// 	.setTopics(topic)
	// 	.setValueOnlyDeserializer(new SimpleStringSchema())
	// 	.build();

	// 	return consumer;
	// }

   private static final class Splitter implements FlatMapFunction<FileDataEntry, FileDataEntry> {

		//Counter Metrics
		private Counter counter;

		@Override
		public void flatMap(FileDataEntry value, Collector<FileDataEntry> out) throws Exception {
			// normalize and split the line
			String word = value.getPayLoad();
			// String word = "value";

			// System.out.println(word);
			if (word.equals("kill")) {
				throw new Exception("Killing the job");
			}

			// out.collect(new FileDataEntry("processed", "1111", Instant.now().toEpochMilli()));
			if (word.charAt(0) != 'a') {
				out.collect(new FileDataEntry(word.toUpperCase(), value.getMsgId(), value.getSourceInTimestamp()));
			} else {
				out.collect(new FileDataEntry(word.toLowerCase(), value.getMsgId(),  value.getSourceInTimestamp()));
			}
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

	private static final class Splitter_Revers implements MapFunction<FileDataEntry, FileDataEntry> {

		@Override
		public FileDataEntry map(FileDataEntry value) throws Exception {
			// normalize and split the line
			String word = value.getPayLoad();

			if (word.charAt(0) != 'A') {
				return new FileDataEntry(word.toLowerCase(), value.getMsgId(), value.getSourceInTimestamp());
			} else {
				return new FileDataEntry(word.toUpperCase(), value.getMsgId(),  value.getSourceInTimestamp());
			}
		}
	}




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