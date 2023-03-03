package org.apache.flink;

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

        final ParameterTool params = ParameterTool.fromArgs(args);
       
        Properties p = new Properties();

        String inputFilePath = "./test1.csv";

		TimeUnit.MILLISECONDS.sleep(40000);

        // data source
        SourceFromFile sourceFromFile = new SourceFromFile(inputFilePath, scalingFactor, inputRate, numData);

        // System.out.println("On port: " + 9998);
        DataStream<FileDataEntry> dataStream = env
            .addSource(sourceFromFile, "Source");

		DataStream<FileDataEntry> flatmappedStream = dataStream
            .flatMap(new Splitter())
			.setParallelism(2);

		// DataStream<FileDataEntry> mappedStream = flatmappedStream
		// .map(new Splitter_Revers())
		// .setParallelism(2);

		// DataStream<FileDataEntry> joinedStream = flatmappedStream
		// .join(mappedStream);

		System.out.println("Start!");
        flatmappedStream.addSink(new SinkFunction(p, numData, "EO", "TEST_Sink10_1", true)).name("sink").setParallelism(1);
		
		env.execute("parallel");
   }

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
}