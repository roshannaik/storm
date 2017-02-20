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

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.spout.ConstSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Map;


public class ConstSpoutOnlyTopo {

    public static final String TOPOLOGY_NAME = "ConstSpoutOnlyTopo";
    public static final String SPOUT_ID = "constSpout";


    public static StormTopology getTopology() {

        // 1 -  Setup Const Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("str");

        // 2 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, 1);
        return builder.createTopology();
    }

    /**
     * ConstSpout only topology  (no bolts)
     */
    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            // submit topology to local cluster
            LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, getTopology());
            Thread.sleep(20000); // let run for a few seconds
            Helper.killAndExit(cluster, TOPOLOGY_NAME);
        } else {
            Integer pollInterval = Integer.parseInt(args[0]); // in seconds
            Integer duration = Integer.parseInt(args[1]);  // in seconds

            // submit to real cluster
            Map clusterConf = Utils.readStormConfig();
            Map conf = Utils.readCommandLineOpts();

            StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, getTopology() );

            Helper.collectMetricsAndKill(TOPOLOGY_NAME, pollInterval, duration, clusterConf);
        }
    }
}
