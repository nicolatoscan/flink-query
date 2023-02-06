// package org.apache.flink.metrics.prometheus;

// import org.apache.flink.annotation.VisibleForTesting;
// import org.apache.flink.metrics.MetricConfig;
// import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
// import org.apache.flink.metrics.reporter.MetricReporterFactory;
// import org.apache.flink.util.AbstractID;
// import org.apache.flink.util.NetUtils;
// import org.apache.flink.util.StringUtils;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.apache.flink.metrics.prometheus.PrometheusReporter.*;

// import org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.*;

// import java.util.Arrays;
// import java.util.Collections;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Properties;

// /** {@link MetricReporterFactory} for {@link PrometheusReporter}. */
// @InterceptInstantiationViaReflection(
//         reporterClassName = "org.apache.flink.metrics.prometheus.PrometheusReporter")
// public class PrometheusReporterFactory implements MetricReporterFactory {

//     private static final Logger LOG =
//              LoggerFactory.getLogger(PrometheusReporterFactory.class);

//     // static final String ARG_PORT = "port";
//     // private static final String DEFAULT_PORT = "9090";

//     @Override
//     public PrometheusReporter createMetricReporter(Properties properties) {
//         // MetricConfig metricConfig = (MetricConfig) properties;
//         // String portsConfig = metricConfig.getString(ARG_PORT, DEFAULT_PORT);
//         // Iterator<Integer> ports = NetUtils.getPortRangeFromString(portsConfig);

//         MetricConfig metricConfig = (MetricConfig) properties;
//          String host = metricConfig.getString(PrometheusPushGatewayReporterOptions.HOST.key(), PrometheusPushGatewayReporterOptions.HOST.defaultValue());
//          int port = metricConfig.getInteger(PrometheusPushGatewayReporterOptions.PORT.key(), PrometheusPushGatewayReporterOptions.PORT.defaultValue());
//          String configuredJobName = metricConfig.getString(PrometheusPushGatewayReporterOptions.JOB_NAME.key(), PrometheusPushGatewayReporterOptions.JOB_NAME.defaultValue());
//          boolean randomSuffix =
//                  metricConfig.getBoolean(
//                          PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX.key(), PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX.defaultValue());
//          boolean deleteOnShutdown =
//                  metricConfig.getBoolean(
//                          PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN.key(), PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN.defaultValue());
//          Map<String, String> groupingKey =
//                  parseGroupingKey(
//                          metricConfig.getString(PrometheusPushGatewayReporterOptions.GROUPING_KEY.key(), PrometheusPushGatewayReporterOptions.GROUPING_KEY.defaultValue()));
 
//          if (host == null || host.isEmpty() || port < 1) {
//              throw new IllegalArgumentException(
//                      "Invalid host/port configuration. Host: " + host + " Port: " + port);
//          }
 
//          String jobName = configuredJobName;
//          if (randomSuffix) {
//              jobName = configuredJobName + new AbstractID();
//          }
 
//          List<String> filterMetrics = Arrays.asList(
//                  metricConfig.getString(PrometheusPushGatewayReporterOptions.FILTER_METRICS.key(),
//                          PrometheusPushGatewayReporterOptions.FILTER_METRICS.defaultValue()).split(","));
 
//          LOG.info(
//                  "Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName:{}, randomJobNameSuffix:{}, deleteOnShutdown:{}, groupingKey:{}, filterMetrics: {}}",
//                  host,
//                  port,
//                  jobName,
//                  randomSuffix,
//                  deleteOnShutdown,
//                  groupingKey,
//                  filterMetrics);

//         return new PrometheusReporter(port);
//     }

//     @VisibleForTesting
//      static Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
//          if (!groupingKeyConfig.isEmpty()) {
//              Map<String, String> groupingKey = new HashMap<>();
//              String[] kvs = groupingKeyConfig.split(";");
//              for (String kv : kvs) {
//                  int idx = kv.indexOf("=");
//                  if (idx < 0) {
//                      LOG.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
//                      continue;
//                  }
 
//                  String labelKey = kv.substring(0, idx);
//                  String labelValue = kv.substring(idx + 1);
//                  if (StringUtils.isNullOrWhitespaceOnly(labelKey)
//                          || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
//                      LOG.warn(
//                              "Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty",
//                              labelKey,
//                              labelValue);
//                      continue;
//                  }
//                  groupingKey.put(labelKey, labelValue);
//              }
 
//              return groupingKey;
//          }
 
//          return Collections.emptyMap();
//      }
// }
