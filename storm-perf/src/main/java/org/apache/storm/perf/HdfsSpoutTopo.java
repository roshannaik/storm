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

package org.apache.storm.perf;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.perf.bolt.DevNullBolt;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


import java.util.Map;

// Copies data from Kafka to Hdfs
public class HdfsSpoutTopo {

    public static final String TOPOLOGY_NAME = "HdfsSpoutTopo";
    public static final String SPOUT_ID = "hdfsSpout";
    public static final String BOLT_ID = "nullBolt";

    public static final String SPOUT_COUNT = "spout.count";
    public static final String BOLT_COUNT = "bolt.count";


    public static StormTopology getTopology(Map conf) {

        // 1 -  Setup HDFS Spout   --------
        HdfsSpout spout = new HdfsSpout().withOutputFields("str");

        // 2 -  Setup DevNull Bolt   --------
        DevNullBolt bolt = new DevNullBolt();

        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout,  Helper.getInt(conf, SPOUT_COUNT, 1) );
        builder.setBolt(BOLT_ID, bolt,  Helper.getInt(conf, BOLT_COUNT, 1 ) )
                .localOrShuffleGrouping(SPOUT_ID);

        return builder.createTopology();
    }


    /**
     * HdfsSpout -> DevNull
     */
    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            LocalCluster cluster = new LocalCluster();
            Map conf = Utils.findAndReadConfigFile("./conf/HdfsSpoutTopo.yaml");
            new Config();
            cluster.submitTopology(TOPOLOGY_NAME, conf, HdfsSpoutTopo.getTopology(conf));
            Thread.sleep(20000); // let run for a few seconds
            Helper.killAndExit(cluster, TOPOLOGY_NAME);
        } else {
            Integer pollInterval = Integer.parseInt(args[0]); // in seconds
            Integer duration = Integer.parseInt(args[1]);  // in seconds
            String topoConfigFile = args[2];
            Map clusterConf = Utils.readStormConfig();
            Map conf = Utils.findAndReadConfigFile(topoConfigFile); // load topo conf
            conf.putAll(Utils.readCommandLineOpts()); // load cmd line opts and override settings from conf

            StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, getTopology(conf) );

            Helper.collectMetricsAndKill(TOPOLOGY_NAME, pollInterval, duration, clusterConf);
        }
    }
}
