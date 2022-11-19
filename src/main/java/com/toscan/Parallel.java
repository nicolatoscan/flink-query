package com.toscan;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Parallel {

   public static void main(String[] args) throws Exception {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
       
      System.out.println("On port: " + 9998);
      DataStream<Tuple2<String, Integer>> dataStream = env
            .socketTextStream("localhost", 9998)
            .flatMap(new Splitter())
            .keyBy(value -> value.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum(1);

      dataStream.print();

      env.execute("Window WordCount");
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