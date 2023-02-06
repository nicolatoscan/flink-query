package org.apache.flink;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class Kafka_reciever_sender {
	
	public static void main(String[] args) throws Exception {
		
		String server = "localhost:9092";
		String inputTopic = "testtopic";
		String inputTopic2 = "testtopic";
		String outputTopic = "testtopic_output";
		     
		StramStringOperation(server,inputTopic,inputTopic2,outputTopic);
		
	}
	
	public static class StringCapitalizer implements MapFunction<String, String> {
	    @Override
	    public String map(String data) throws Exception {
	        return data.toUpperCase();
	    }
	}

	public static class Tuple2String implements MapFunction<Tuple2<String, Integer>, String> {
		
		@Override
		public String map(Tuple2<String, Integer> tuple) throws Exception {
			return tuple.toString();
		}

	}
	
	public static void StramStringOperation(String server,String inputTopic,String inputTopic2,String outputTopic) throws Exception {
	    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 5000));
		
	    KafkaSource<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
		KafkaSource<String> flinkKafkaConsumer2 = createStringConsumerForTopic(inputTopic2, server);

	    // KafkaSink<String> flinkKafkaProducer = createStringProducer(outputTopic, server);

		DataStream<Tuple2<String, Integer>> stringInputStream = environment.fromSource(flinkKafkaConsumer, WatermarkStrategy.noWatermarks(), "Kafka Source")
		.flatMap(new Splitter())
		.keyBy(value -> value.f0)
		.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
		.sum(1);

		DataStream<Tuple2<String, Integer>> stringInputStream2 = environment.fromSource(flinkKafkaConsumer2, WatermarkStrategy.noWatermarks(), "Kafka Source")
		.flatMap(new Splitter())
		.keyBy(value -> value.f0)
		.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
		.sum(1);

		DataStream<Tuple2<String, Integer>> results = stringInputStream.union(stringInputStream2); // .map(Tuple2String);
		
		// results.sinkTo(flinkKafkaProducer);
		results.print();		
	  
	    // stringInputStream.map(new StringCapitalizer()).sinkTo(flinkKafkaProducer);
	   
	    environment.execute("Union text");
	}
	
	public static KafkaSource<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaAddress);

		KafkaSource<String> consumer = KafkaSource
		.<String>builder()
		.setProperties(props)
		.setTopics(topic)
		.setValueOnlyDeserializer(new SimpleStringSchema())
		.build();

		return consumer;
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

	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
      @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
               
               System.out.println(word);
               if (word.equals("kill")) {
                  throw new Exception("Killing the job");
               }
               out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
   }

}