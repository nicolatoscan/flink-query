package org.apache.flink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class App 
{
    public static void main(String[] args) throws Exception
    {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        env.getConfig().setGlobalJobParameters(params);
        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        if (params.has("output")) {
           counts.writeAsCsv(params.get("output"), "\n", " ");
           env.execute("WordCount Example");
        } else {
           System.out.println("Printing result to stdout. Use --output to specify output path.");
           counts.print();
        }
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
           String[] tokens = value.toLowerCase().split("\\W+");
           for (String token : tokens) {
              if (token.length() > 0) {
                 out.collect(new Tuple2<>(token, 1));
              }
           }
        }
     }
}
