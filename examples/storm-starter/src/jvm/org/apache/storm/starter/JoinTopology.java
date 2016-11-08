/**
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

package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.bolt.JoinBolt;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.bolt.SlidingWindowSumBolt;
import org.apache.storm.starter.spout.RandomIntegerSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

import java.util.concurrent.TimeUnit;

public class JoinTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        BaseWindowedBolt bolt = new JoinBolt(JoinBolt.StreamSelector.STREAM, "stream1")
                .withWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS))
                .withTimestampField("ts")
                .withLag(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
        builder.setSpout("integerSpout", new RandomIntegerSpout(), 1);
        builder.setBolt("joinBolt", bolt, 1).shuffleGrouping("integerSpout");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("joinBolt");
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(40000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
