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

package org.apache.storm.perf.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ThroughputMeter;

import java.util.Map;


public class DevNullBolt extends BaseRichBolt {
    private OutputCollector collector;
    ThroughputMeter meter;
    public static final  org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DevNullBolt.class);
    long count =0;
    int printFreq =0;

    public DevNullBolt() {
        super();
        printFreq= 10_000_000;
    }

    public DevNullBolt(int printFreq) {
        super();
        this.printFreq = printFreq;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);
        if(meter==null) {
            ++count;
            if(count==5_000_000) {
                this.meter = new ThroughputMeter("DevNull Bolt", printFreq);
            }
        } else {
            meter.record();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (meter!=null)
            LOG.error(" =====> %,d k/s ", meter.stop()/1000);

    }
}
