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
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.bolt.DevNullBolt;
import org.apache.storm.perf.bolt.IdBolt;
import org.apache.storm.perf.spout.ConstSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class ConstSpoutIDBoltNullBolt {

    public static final String TOPOLOGY_NAME = "ConstSpoutIDBoltNullBoltTopo";
    public static final String SPOUT_ID = "constSpout";
    public static final String BOLT1_ID = "idBolt";
    public static final String BOLT1b_ID = "idBolt2";
    public static final String BOLT2_ID = "nullBolt";

    public static final String BOLT1_COUNT = "bolt1.count";
    public static final String BOLT2_COUNT = "bolt2.count";
    public static final String SPOUT_COUNT = "spout.count";

    public static StormTopology getTopology(Map conf, int printFreq) {

        // 1 -  Setup Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("str");

        // 2 -  Setup IdBolt 7 DevNullBolt   --------
        IdBolt bolt1 = new IdBolt();
        IdBolt bolt1b = new IdBolt();

        DevNullBolt bolt2 = new DevNullBolt(printFreq);


        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, spout, 1 );

        builder.setBolt(BOLT1_ID, bolt1, 1)
                .localOrShuffleGrouping(SPOUT_ID);

        builder.setBolt(BOLT2_ID, bolt2, Helper.getInt(conf, BOLT2_COUNT, 1))
                .localOrShuffleGrouping(BOLT1_ID);

        return builder.createTopology();
    }

    /**
     * ConstSpout -> DevNull Bolt
     */
    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            // submit topology to local cluster
            Config conf = new Config();
            conf.setNumAckers(0);
            int printFreq = 15_000_000;
//            int printFreq = 6_000_000;

            Helper.runOnLocalCluster(TOPOLOGY_NAME, getTopology(conf,printFreq), conf);

            while(true) {  // run indefinitely
                Thread.sleep(20_000_000);
            }
        } else {
            Integer duration = Integer.parseInt(args[0]);  // in seconds
            Integer pollInterval = 60; // in seconds

            int printFreq = Integer.parseInt(args[1]);
            // submit to real cluster
            Map stormConf = Utils.readStormConfig();
            StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, stormConf, getTopology(stormConf,printFreq) );
            Helper.collectMetricsAndKill(TOPOLOGY_NAME, pollInterval, duration);
        }
    }
}
