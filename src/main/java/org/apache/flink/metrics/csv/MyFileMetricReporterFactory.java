package org.apache.flink.metrics.csv;

import java.util.Properties;

import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

public class MyFileMetricReporterFactory implements MetricReporterFactory {

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new MyFileMetricReporter();
    }

}
