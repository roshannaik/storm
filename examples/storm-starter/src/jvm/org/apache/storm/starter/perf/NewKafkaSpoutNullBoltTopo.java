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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.starter.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


// Copies data from Kafka to Hdfs
public class NewKafkaSpoutNullBoltTopo {

    // configs - topo parallelism
    public static final String SPOUT_NUM = "spout.count";
    public static final String BOLT_NUM = "bolt.count";

    // configs - kafka spout
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String KAFKA_BROKER = "kafka.broker";


    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_BOLT_NUM = 1;

    // names
    public static final String TOPOLOGY_NAME = "NewKafkaNullBoltTopo";
    public static final String SPOUT_ID = "kafkaSpout";
    public static final String BOLT_ID = "devNullBolt";

    // tuple field names
    public static final String DATA = "str";
    public static final String TOPIC = "topic";


    public static StormTopology getTopology(Map config) {

        final int spoutNum = getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);
        // 1 -  Setup Kafka Spout   --------
        kafka.api.OffsetRequest.EarliestTime();
        final String broker = getStr(config, KAFKA_BROKER);
        final String topicName = getStr(config, KAFKA_TOPIC);

        Map<String, Object> kafkaConsumerProps = getKafkaConsumerProps(broker);
        Fields outputFields = new Fields(DATA, TOPIC);
        KafkaSpoutStreams kafkaSpoutStreams = new KafkaSpoutStreams.Builder(outputFields, "default", new String[]{topicName} ).build();
        KafkaSpoutTuplesBuilder<String, String> tupleBuilder = new KafkaSpoutTuplesBuilder.Builder<>(new SimpleTupleBuilder(topicName)).build();

        KafkaSpoutConfig kafkaSpoutConfig = new KafkaSpoutConfig.Builder<>(kafkaConsumerProps, kafkaSpoutStreams, tupleBuilder)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)
                .build();

        KafkaSpout<String,String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

        // 2 -   DevNull Bolt   --------
        DevNullBolt bolt = new DevNullBolt();

        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, kafkaSpout, spoutNum);
        builder.setBolt(BOLT_ID, bolt, boltNum)
                .localOrShuffleGrouping(SPOUT_ID);

        return builder.createTopology();
    }


    public static Map<String,Object> getKafkaConsumerProps(String broker) {
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "false");
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, broker);
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "NewKafkaSpoutNullBoltTopo");
        return props;
    }

    public static int getInt(Map map, Object key, int def) {
        return Utils.getInt(Utils.get(map, key, def));
    }

    public static String getStr(Map map, Object key) {
        return (String) map.get(key);
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

//    // local cluster
//    public static void main(String[] args) throws Exception {
//        // 1 - Submit topology
//        Config conf = new Config();
//        conf.put("kafka.topic","kafka_hdfs");
//        conf.put("kafka.broker","172.18.128.67:6667");
//        conf.put("spout.count",1);
//        conf.put("bolt.count",1);
//        LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, getTopology(conf));
//        Thread.sleep(20000); // let run for a few seconds
//        Helper.killAndExit(cluster, TOPOLOGY_NAME);
//    }


    public static class SimpleTupleBuilder extends KafkaSpoutTupleBuilder<String,String> {
        public SimpleTupleBuilder(String topic) {
            super(topic);
        }

        @Override
        public List<Object> buildTuple(ConsumerRecord<String, String> consumerRecord) {
            return new Values(consumerRecord.value(),  consumerRecord.topic());
        }
    }
}
