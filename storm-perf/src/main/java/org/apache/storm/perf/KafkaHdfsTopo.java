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
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringMultiSchemeWithTopic;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;

// Copies data from Kafka to Hdfs
public class KafkaHdfsTopo {

  // configs - topo parallelism
  public static final String SPOUT_NUM = "spout.count";
  public static final String BOLT_NUM = "bolt.count";
  // configs - kafka spout
  public static final String KAFKA_TOPIC = "kafka.topic";
  public static final String ZOOKEEPER_URI = "zk.uri";
  // configs - hdfs bolt
  public static final String HDFS_URI = "hdfs.uri";
  public static final String HDFS_PATH = "hdfs.dir";
  public static final String HDFS_BATCH = "hdfs.batch";


  public static final int DEFAULT_SPOUT_NUM = 1;
  public static final int DEFAULT_BOLT_NUM = 1;
  public static final int DEFAULT_HDFS_BATCH = 1000;

  // names
  public static final String TOPOLOGY_NAME = "KafkaHdfsTopo";
  public static final String SPOUT_ID = "kafkaSpout";
  public static final String BOLT_ID = "hdfsBolt";



  public static StormTopology getTopology(Map config) {

    final int spoutNum = getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int boltNum = getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    final int hdfsBatch = getInt(config, HDFS_BATCH, DEFAULT_HDFS_BATCH);

    // 1 -  Setup Kafka Spout   --------
    String zkConnString = getStr(config, ZOOKEEPER_URI);
    String topicName = getStr(config, KAFKA_TOPIC);

    BrokerHosts brokerHosts = new ZkHosts(zkConnString);
    SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topicName, "/" + topicName, UUID.randomUUID().toString());
    spoutConfig.scheme = new StringMultiSchemeWithTopic();
    spoutConfig.ignoreZkOffsets = true;

    KafkaSpout spout = new KafkaSpout(spoutConfig);

    // 2 -  Setup HFS Bolt   --------
    String Hdfs_url = getStr(config, HDFS_URI);
    RecordFormat format = new LineWriter("str");
    SyncPolicy syncPolicy = new CountSyncPolicy(hdfsBatch);
    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.GB);


    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(getStr(config,HDFS_PATH) );

    // Instantiate the HdfsBolt
    HdfsBolt bolt = new HdfsBolt()
            .withFsUrl(Hdfs_url)
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(format)
            .withRotationPolicy(rotationPolicy)
            .withSyncPolicy(syncPolicy);


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


    /** Copies text file content from sourceDir to destinationDir. Moves source files into sourceDir after its done consuming */
    public static void main(String[] args) throws Exception {
        // 1 - Submit topology
        if (args.length == 0) {
            System.err.println("Please specify topology config file as argument");
            return;
        }
        String confFile = args[0];
        Map topoConf = Utils.findAndReadConfigFile(confFile);

        Config conf = new Config();
        conf.putAll(Utils.readStormConfig());               // Storm.yaml & cmd line options
        conf.putAll(topoConf);                              // combine Topology config file

        Integer pollInterval = Integer.parseInt(args[1]); // in seconds
        Integer duration = Integer.parseInt(args[2]);     // in seconds

        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, getTopology(topoConf));

        // 2 - Print metrics every 60 sec, killAndExit topology after 30 min
        Helper.collectMetricsAndKill(TOPOLOGY_NAME, pollInterval, duration, conf);

    }


//  public static void main(String[] args) throws Exception {
//    if(args.length <= 0) {
//      // submit topology to local cluster
//      LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, HdfsBoltTopo.getTopology("conf/HdfsSpoutTopo.yaml"));
//      Thread.sleep(20000); // let run for a few seconds
//      Helper.killAndExit(cluster, TOPOLOGY_NAME);
//    } else {
//      // submit to real cluster
//      Integer pollInterval = Integer.parseInt(args[0]); // in seconds
//      Integer duration = Integer.parseInt(args[1]);  // in seconds
//      String topoConfigFile = args[2];
//
//      Map clusterConf = Utils.readStormConfig();
//      Map conf = Utils.readCommandLineOpts();
//
//      StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, getTopology(topoConfigFile) );
//
//      Helper.collectMetricsAndKill(TOPOLOGY_NAME, pollInterval, duration, clusterConf);
//    }
//  }


    public static class LineWriter implements RecordFormat {
        private String lineDelimiter = System.lineSeparator();
        private String fieldName;

        public LineWriter(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Overrides the default record delimiter.
         *
         * @param delimiter
         * @return
         */
        public LineWriter withLineDelimiter(String delimiter){
            this.lineDelimiter = delimiter;
            return this;
        }

        @Override
        public byte[] format(Tuple tuple) {
            return (tuple.getValueByField(fieldName).toString() +  this.lineDelimiter).getBytes();
        }
    }
}