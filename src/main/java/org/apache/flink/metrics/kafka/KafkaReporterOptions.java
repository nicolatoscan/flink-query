package org.apache.flink.metrics.kafka;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * <h4>FlinkMetricTutorial</h4>
 * <p>向kafka生产数据的配置项</p>
 *
 * @author : guan
 * @date : 2022-06-13 14:59
 **/
public class KafkaReporterOptions {
    public static final ConfigOption<String> CLUSTER = ConfigOptions
            .key("cluster")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of flink cluster");


    public static final ConfigOption<String> SERVERS = ConfigOptions
            .key("bootstrap.servers")
            .stringType()
            .defaultValue("localhost:9092")
            .withDescription("The kafka bootstrap server host.");

    public static final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic")
            .stringType()
            .defaultValue("metric")
            .withDescription("The metrics topic.");

    public static final ConfigOption<String> KEY_BY = ConfigOptions
            .key("keyBy")
            .stringType()
            .defaultValue("")
            .withDescription("The key name of kafka producer");
}
