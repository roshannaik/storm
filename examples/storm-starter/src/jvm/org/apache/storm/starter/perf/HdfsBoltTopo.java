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


package org.apache.storm.starter.perf;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.starter.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.Map;

// Writes data to Hdfs
public class HdfsBoltTopo {

    // configs - topo parallelism
    public static final String SPOUT_NUM = "spout.count";
    public static final String BOLT_NUM = "bolt.count";

    // configs - hdfs bolt
    public static final String HDFS_URI = "hdfs.uri";
    public static final String HDFS_PATH = "hdfs.dir";
    public static final String HDFS_BATCH = "hdfs.batch";

    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_BOLT_NUM = 1;
    public static final int DEFAULT_HDFS_BATCH = 1000;

    // names
    public static final String TOPOLOGY_NAME = "HdfsBoltTopo";
    public static final String SPOUT_ID = "GenSpout";
    public static final String BOLT_ID = "hdfsBolt";


    public static StormTopology getTopology(Map topoConf) {
        final int hdfsBatch = Helper.getInt(topoConf, HDFS_BATCH, DEFAULT_HDFS_BATCH);

        kafka.api.OffsetRequest.EarliestTime();


        // 1 -  Setup StringGen Spout   --------
        StringGenSpout spout = new StringGenSpout(100).withFieldName("str");


        // 2 -  Setup HFS Bolt   --------
        String Hdfs_url = Helper.getStr(topoConf, HDFS_URI);
        RecordFormat format = new LineWriter("str");
        SyncPolicy syncPolicy = new CountSyncPolicy(hdfsBatch);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.GB);
        final int spoutNum = Helper.getInt(topoConf, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = Helper.getInt(topoConf, BOLT_NUM, DEFAULT_BOLT_NUM);

        // Use default, Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(Helper.getStr(topoConf, HDFS_PATH) );

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


    /** Spout generates random strings and HDFS bolt writes them to a text file */
    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            // submit topology to local cluster
            Map topoConf = Utils.findAndReadConfigFile("conf/HdfsSpoutTopo.yaml");
            LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, HdfsBoltTopo.getTopology(topoConf));
            Thread.sleep(20000); // let run for a few seconds
            Helper.killAndExit(cluster, TOPOLOGY_NAME);
        } else {
            // submit to real cluster
            Integer pollInterval = Integer.parseInt(args[0]); // in seconds
            Integer duration = Integer.parseInt(args[1]);  // in seconds
            String topoConfigFile = args[2]; // path

            Map stormConf = Utils.readStormConfig();
            Map topoConf = Utils.findAndReadConfigFile(topoConfigFile);
            stormConf.putAll(topoConf);
            StormSubmitter.submitTopology(TOPOLOGY_NAME, stormConf, getTopology(topoConf) );

            Helper.collectMetricsAndKill(TOPOLOGY_NAME, pollInterval, duration, stormConf);
        }
    }


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

        public byte[] format(Tuple tuple) {
            return (tuple.getValueByField(fieldName).toString() +  this.lineDelimiter).getBytes();
        }
    }

}