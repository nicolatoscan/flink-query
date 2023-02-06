package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class WindowWordCountJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		DataStream<Tuple2<String, Integer>> dataStream = env
				.addSource(new WordSource())
				.flatMap(new Splitter())
                .setParallelism(2)
                .keyBy(value -> value.f0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
				.sum(1)
                ;
        
        // dataStream.addSink(flinkKafkaProducer);
        // dataStream.sinkTo(flinkKafkaProducer);
        dataStream.print().setParallelism(2);
        
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
		env.execute("Window WordCount");
    }

    public static KafkaSink<String> createStringProducer(String topic, String kafkaAddress){

		return KafkaSink.<String>builder()
        .setBootstrapServers(kafkaAddress)
		.setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(topic)
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
        )
        .build();
	}

    public static final class WordSource extends RichSourceFunction<String> {

        private static final long serialVersionUID = 1L;
        private transient Random random;
        private transient int waitMS; // wait milliseconds
        private transient boolean isCancel;
        private transient String[] lines;

        private transient Counter eventCounter;
        private transient Histogram valueHistogram;

        // private MetricRegistry metricRegistry = new MetricRegistry();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.waitMS = parameters.getInteger(ConfigOptions.key("application.parallelism")
            .intType()
            .defaultValue(3000), 
            3000); 
            random = new Random();
            InputStream is = getClass().getClassLoader().getResourceAsStream("test.txt");
            Scanner s = new Scanner(is);
            ArrayList<String> raw = new ArrayList<>();
            while(s.hasNext()) {
                String line = s.nextLine();
                if(line.trim().length() > 0) {
                    raw.add(line);
                }
            }
            this.lines = raw.toArray(new String[]{});

            eventCounter = getRuntimeContext().getMetricGroup().counter("events");
            valueHistogram =
                    getRuntimeContext()
                            .getMetricGroup()
                            .histogram("value_histogram", new DescriptiveStatisticsHistogram(10_000_000));
        }
        
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (! isCancel) {
                eventCounter.inc();
                TimeUnit.MILLISECONDS.sleep(Math.max(100, random.nextInt(this.waitMS)));
                String text = this.lines[random.nextInt(this.lines.length - 1)];
                valueHistogram.update(text.length());
                ctx.collect(text);
            }
        }

        @Override
        public void cancel() {
            isCancel = true;
        }
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {

                if (word.equals("kill")) {
                    throw new Exception("Killing the job");
                }
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
