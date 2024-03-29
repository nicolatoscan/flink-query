package org.apache.flink.metrics.prometheus;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 import io.prometheus.client.CollectorRegistry;
 import io.prometheus.client.exporter.PushGateway;
 import org.apache.flink.annotation.PublicEvolving;
 import org.apache.flink.metrics.Metric;
 import org.apache.flink.metrics.reporter.InstantiateViaFactory;
 import org.apache.flink.metrics.reporter.MetricReporter;
 import org.apache.flink.metrics.reporter.Scheduled;
 import org.apache.flink.util.Preconditions;
 
 import java.io.IOException;
 import java.util.List;
 import java.util.Map;
 
 /**
  * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus {@link PushGateway}.
  */
 @PublicEvolving
 @InstantiateViaFactory(
         factoryClassName =
                 "org.apache.metrics.prometheus.PrometheusPushGatewayReporterFactory")
 public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {
 
     private final PushGateway pushGateway;
     private final String jobName;
     private final Map<String, String> groupingKey;
     private final boolean deleteOnShutdown;
     private final List<String> filterMetrics;
 
 
     PrometheusPushGatewayReporter(
             String host,
             int port,
             String jobName,
             Map<String, String> groupingKey,
             final boolean deleteOnShutdown,
             List<String> filterMetrics) {
         this.pushGateway = new PushGateway(host + ':' + port);
         this.jobName = Preconditions.checkNotNull(jobName);
         this.groupingKey = Preconditions.checkNotNull(groupingKey);
         this.deleteOnShutdown = deleteOnShutdown;
         this.filterMetrics = filterMetrics;
 
     }
 
     @Override
     public void report() {
         try {
             CollectorRegistry cr = new CollectorRegistry();
             Boolean pushFlag = false;
             for(String metricsName : this.collectorsWithCountByMetricName.keySet()){
                 for (String filterName : filterMetrics){
                     if (metricsName.contains(filterName)){
                         cr.register(this.collectorsWithCountByMetricName.get(metricsName).getKey());
                         pushFlag = true;
                     }
                 }
             }
        //      log.warn("pushGateway: {}, jobName: {}, groupingKey: {}, deleteOnShutdown: {}, filterMetrics: {}", this.pushGateway, this.jobName, this.groupingKey,
        //  this.deleteOnShutdown, this.filterMetrics);
             if (pushFlag){
                 pushGateway.push(cr, jobName, groupingKey);
             }
         } catch (Exception e) {
             log.warn(
                     "Failed to push metrics to PushGateway with jobName {}, groupingKey {}.",
                     jobName,
                     groupingKey,
                     e);
         }
     }
 
 
 
     @Override
     public void close() {
         if (deleteOnShutdown && pushGateway != null) {
             try {
                 pushGateway.delete(jobName, groupingKey);
             } catch (IOException e) {
                 log.warn(
                         "Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.",
                         jobName,
                         groupingKey,
                         e);
             }
         }
         super.close();
     }
 }
