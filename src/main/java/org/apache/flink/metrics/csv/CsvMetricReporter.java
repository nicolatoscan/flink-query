// package org.apache.flink.metrics.csv;

// import org.apache.flink.metrics.Metric;
// import org.apache.flink.metrics.MetricConfig;
// import org.apache.flink.metrics.MetricGroup;
// import org.apache.flink.metrics.reporter.MetricReporter;

// import java.io.FileWriter;
// import java.io.IOException;
// import java.util.Map;

// public class CsvMetricReporter implements MetricReporter {

//     private FileWriter writer;

//     @Override
//     public void open(MetricConfig config) {
//         try {
//             writer = new FileWriter("metrics.csv");
//         } catch (IOException e) {
//             e.printStackTrace();
//         }
//     }

//     @Override
//     public void close() {
//         try {
//             writer.close();
//         } catch (IOException e) {
//             e.printStackTrace();
//         }
//     }

//     @Override
//     public void report() {
//         for (Map.Entry<String, String> entry : group.getAllVariables().entrySet()) {
//             try {
//                 writer.write(group.toString() + "," + entry.getKey() + "," + entry.getValue());
//                 writer.write("\n");
//             } catch (IOException e) {
//                 e.printStackTrace();
//             }
//         }
//     }

//     @Override
//     public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
//         // TODO
//     }

//     @Override
//     public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
//         // TODO
//     }
// }
