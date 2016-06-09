/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.starter.perf;


import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.hdfs.spout.TextFileReader;
import org.apache.storm.starter.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class HdfsSpoutNullBoltTopo {
    // names
    public static final String TOPOLOGY_NAME = "HdfsSpoutNullBoltTopo";
    public static final String SPOUT_ID = "hdfsSpout";
    public static final String BOLT_ID = "devNullBolt";

    // configs - topo parallelism
    public static final String SPOUT_NUM = "spout.count";
    public static final String BOLT_NUM = "bolt.count";

    public static final String HDFS_SRC = "hdfs.src";
    public static final String HDFS_DONE = "hdfs.done";
    public static final String HDFS_BAD = "hdfs.bad";


    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_BOLT_NUM = 1;


    public static StormTopology getTopology(Map config) {

        final int spoutNum = Helper.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = Helper.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);
        // 1 -  Setup Hdfs Spout   --------
        HdfsSpout spout = new HdfsSpout().withOutputFields(TextFileReader.defaultFields);

        // 2 -   DevNull Bolt   --------
        DevNullBolt bolt = new DevNullBolt();

        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, bolt, boltNum)
                .localOrShuffleGrouping(SPOUT_ID);

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        // 1 - Submit topology
        if (args.length == 0) {
            System.err.println("args: confFile pollInterval duration");
            return;
        }
        String confFile = args[0];
        Integer pollInterval = Integer.parseInt(args[1]); // in seconds
        Integer duration = Integer.parseInt(args[2]);  // in seconds

        Map conf = Utils.readStormConfig();               // Storm.yaml & cmd line options
        conf.putAll(Utils.findAndReadConfigFile(confFile)); // Topology config file

        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, getTopology(conf) );
        Helper.collectMetricsAndKill(TOPOLOGY_NAME, pollInterval, duration, conf);
    }
}
