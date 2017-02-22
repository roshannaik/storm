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
 * limitations under the License
 */

package org.apache.storm.perf.utils;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;


public class Helper {
  public static void printMetrics(Nimbus.Client client, String topoName) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    String id = null;
    for (TopologySummary ts: summary.get_topologies()) {
      if (topoName.equals(ts.get_name())) {
        id = ts.get_id();
      }
    }
    if (id == null) {
      throw new Exception("Could not find a topology named " + topoName);
    }
    TopologyInfo info = client.getTopologyInfo(id);
    int uptime = info.get_uptime_secs();
    long acked = 0;
    long failed = 0;
    double weightedAvgTotal = 0.0;
    for (ExecutorSummary exec: info.get_executors()) {
      if ("spout".equals(exec.get_component_id())) {
        SpoutStats stats = exec.get_stats().get_specific().get_spout();
        Map<String, Long> failedMap = stats.get_failed().get(":all-time");
        Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
        Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
        for (String key: ackedMap.keySet()) {
          if (failedMap != null) {
            Long tmp = failedMap.get(key);
            if (tmp != null) {
              failed += tmp;
            }
          }
          long ackVal = ackedMap.get(key);
          double latVal = avgLatMap.get(key) * ackVal;
          acked += ackVal;
          weightedAvgTotal += latVal;
        }
      }
    }
    double avgLatency = weightedAvgTotal/acked;
    System.out.println("uptime: "+uptime+" acked: "+acked+" avgLatency: "+avgLatency+" acked/sec: "+(((double)acked)/uptime+" failed: "+failed));
  }

  public static void kill(Nimbus.Client client, String name) throws Exception {
    KillOptions opts = new KillOptions();
    opts.set_wait_secs(0);
    client.killTopologyWithOpts(name, opts);
  }

  public static void killAndExit(LocalCluster cluster, String topName) throws Exception {
    KillOptions opts = new KillOptions();
    opts.set_wait_secs(0);
    cluster.killTopologyWithOpts(topName, opts);
    cluster.shutdown();
  }

    public static LocalCluster runOnLocalCluster(String topoName, StormTopology topology) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, new Config(), topology);
        return cluster;
    }

    public static int getInt(Map map, Object key, int def) {
        return Utils.getInt(Utils.get(map, key, def));
    }

    public static String getStr(Map map, Object key) {
        return (String) map.get(key);
    }

    public static void collectMetricsAndKill(String topologyName, Integer pollInterval, Integer duration, Map clusterConf) throws Exception {
      Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        BasicMetricsCollector metricsCollector = new BasicMetricsCollector(client, topologyName, clusterConf);

        int times = duration / pollInterval;
        metricsCollector.collect(client);
        for (int i = 0; i < times; i++) {
            Thread.sleep(pollInterval * 1000);
            metricsCollector.collect(client);
        }
        metricsCollector.close();
        kill(client, topologyName);
    }

    public static void collectLocalMetricsAndKill(LocalCluster localCluster, String topologyName, Integer pollInterval, Integer duration, Map clusterConf) throws Exception {
        BasicMetricsCollector metricsCollector = new BasicMetricsCollector(localCluster, topologyName, clusterConf);

        int times = duration / pollInterval;
        metricsCollector.collect(localCluster);
        for (int i = 0; i < times; i++) {
            Thread.sleep(pollInterval * 1000);
            metricsCollector.collect(localCluster);
        }
        metricsCollector.close();
        killAndExit(localCluster, topologyName);
    }
}
