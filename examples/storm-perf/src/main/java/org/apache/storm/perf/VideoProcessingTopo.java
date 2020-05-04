/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.perf;

import java.util.ArrayList;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.bolt.BlackAndWhiteBolt;
import org.apache.storm.perf.bolt.EdgeDetectBolt;
import org.apache.storm.perf.bolt.FaceDetectBolt;
import org.apache.storm.perf.bolt.ShowVideoBolt;
import org.apache.storm.perf.spout.CameraSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class VideoProcessingTopo {

    public static final String TOPOLOGY_NAME = "VideoProcessTopo";
    public static final String SPOUT_ID = "cameraSpout";

    static StormTopology getTopology(int count) {

        TopologyBuilder builder = makePaths(count);

        return builder.createTopology();
    }

    /**
     * CameraSpout -> EdgeDetect Bolt -> ShowVideo Bolt.
     */
    public static void main(String[] args) throws Exception {
        int pathCount = 1;
        int runTime = 240;
        Config topoConf = new Config();
        topoConf.put(Config.TOPOLOGY_SPOUT_RECVQ_SKIPS, 8);
        topoConf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 1);
        topoConf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16);
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.0005);
        topoConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        topoConf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS, "-Xms8g -Xmx8g");
        topoConf.put(Config.TOPOLOGY_WORKERS, 1);

        if (args.length > 2) {
            System.err.println("args: [pathCount=1] [runDurationSec=240]  [optionalConfFile]");
            return;
        }

        if (args.length > 0) {
            pathCount = Integer.parseInt(args[0]);
        }

        if (args.length > 1) {
            runTime = Integer.parseInt(args[1]);
        }

        if (args.length > 2) {
            topoConf.putAll(Utils.findAndReadConfigFile(args[2]));
        }

        topoConf.putAll(Utils.readCommandLineOpts());

        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(runTime, TOPOLOGY_NAME, topoConf, getTopology(pathCount));
    }


    static TopologyBuilder makePaths(int n) {
        String[] streams = null; // makeStreamNames(n);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new CameraSpout(streams), 1);
        int id = 0;
        for (int i = 0; i < n; i++) {
            String boltId;

            switch (id) {
                case 0:
                    boltId = "FaceDetect" + i;
                    builder.setBolt(boltId, new FaceDetectBolt(), 1)
                        .allGrouping(SPOUT_ID);
                    ++id;
                    break;
                case 1:
                    boltId = "EdgeDetect" + i;
                    builder.setBolt(boltId, new EdgeDetectBolt(), 1)
                        .allGrouping(SPOUT_ID);
                    ++id;
                    break;
                default:
                    boltId = "Black-n-White" + i;
                    builder.setBolt(boltId, new BlackAndWhiteBolt(), 1)
                        .allGrouping(SPOUT_ID);
                    id = 0;
                    break;
            }

            builder.setBolt(boltId + "Viewer", new ShowVideoBolt(boltId), 1)
                .localOrShuffleGrouping(boltId);

        }
        return builder;
    }

    private static String[] makeStreamNames(int n) {
        ArrayList<String> result = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            result.add("s" + i);
        }
        return result.toArray(new String[] {});
    }
}
