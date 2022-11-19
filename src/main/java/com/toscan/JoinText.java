package com.toscan;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class JoinText {

   public static void main(String[] args) throws Exception {

      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 5000));

      DataStream<Tuple2<String, Integer>> ds1 = env
         .socketTextStream("localhost", 9000)
         .flatMap(new Splitter())
         .keyBy(value -> value.f0)
         .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
         .sum(1);

      DataStream<Tuple2<String, Integer>> ds2 = env
         .socketTextStream("localhost", 9001)
         .flatMap(new Splitter())
         .keyBy(value -> value.f0)
         .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
         .sum(1);

      ds1.union(ds2).print();
      env.execute("Union text");
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