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
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringMultiSchemeWithTopic;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.perf.bolt.DevNullBolt;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;


// Copies data from Kafka to Hdfs
public class KafkaSpoutNullBoltTopo {

    // configs - topo parallelism
    public static final String SPOUT_NUM = "spout.count";
    public static final String BOLT_NUM = "bolt.count";

    // configs - kafka spout
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String ZOOKEEPER_URI = "zk.uri";


    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_BOLT_NUM = 1;

    // names
    public static final String TOPOLOGY_NAME = "KafkaTopo";
    public static final String SPOUT_ID = "kafkaSpout";
    public static final String BOLT_ID = "devNullBolt";


    public static StormTopology getTopology(Map config) {

        final int spoutNum = getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);
        // 1 -  Setup Kafka Spout   --------

        String zkConnString = getStr(config, ZOOKEEPER_URI);
        String topicName = getStr(config, KAFKA_TOPIC);

        BrokerHosts brokerHosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new StringMultiSchemeWithTopic();
        spoutConfig.ignoreZkOffsets = true;

        KafkaSpout spout = new KafkaSpout(spoutConfig);

        // 2 -   DevNull Bolt   --------
        DevNullBolt bolt = new DevNullBolt();

        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, bolt, boltNum)
                .localOrShuffleGrouping(SPOUT_ID);

        return builder.createTopology();
    }


    public static int getInt(Map map, Object key, int def) {
        return Utils.getInt(Utils.get(map, key, def));
    }

    public static String getStr(Map map, Object key) {
        return (String) map.get(key);
    }


    /**
     * Copies text file content from sourceDir to destinationDir. Moves source files into sourceDir after its done consuming
     */
    public static void main(String[] args) throws Exception {
        // 1 - Submit topology
        if (args.length == 0) {
            System.err.println("Please specify config file as argument");
            return;
        }
        String confFile = args[0];
        Integer pollInterval = Integer.parseInt(args[1]); // in seconds
        Integer duration = Integer.parseInt(args[2]);  // in seconds

        Config conf = new Config();
        conf.putAll(Utils.readStormConfig());               // Storm.yaml & cmd line options
        conf.putAll(Utils.findAndReadConfigFile(confFile)); // Topology config file
        System.out.println(conf);

        // 2 - Submit and kill
        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, getTopology(conf) );
        Helper.collectMetricsAndKill(TOPOLOGY_NAME, pollInterval, duration, conf);


    }
}
