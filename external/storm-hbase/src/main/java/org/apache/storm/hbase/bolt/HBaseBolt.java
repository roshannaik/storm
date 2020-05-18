/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.hbase.bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic bolt for writing to HBase.
 *
 * <p>Note: Each HBaseBolt defined in a topology is tied to a specific table.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class HBaseBolt extends AbstractHBaseBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseBolt.class);
    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    boolean writeToWAL = true;
    List<Mutation> batchMutations;
    int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;
    int batchSize;
    BatchHelper batchHelper;

    public HBaseBolt(String tableName, HBaseMapper mapper) {
        super(tableName, mapper);
        this.batchMutations = new LinkedList<>();
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public HBaseBolt writeToWAL(boolean writeToWAL) {
        this.writeToWAL = writeToWAL;
        return this;
    }

    public HBaseBolt withConfigKey(String configKey) {
        this.configKey = configKey;
        return this;
    }

    public HBaseBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public HBaseBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalSecs);
    }


    @Override
    public void execute(Tuple tuple) {
        try {
            if (batchHelper.shouldHandle(tuple)) {
                byte[] rowKey = this.mapper.rowKey(tuple);
                ColumnList cols = this.mapper.columns(tuple);
                List<Mutation> mutations =
                    hBaseClient.constructMutationReq(rowKey, cols, writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL);
                batchMutations.addAll(mutations);
                batchHelper.addBatch(tuple);
            }

            if (batchHelper.shouldFlush()) {
                this.hBaseClient.batchMutate(batchMutations);
                LOG.debug("acknowledging tuples after batchMutate");
                batchHelper.ack();
                batchMutations.clear();
            }
        } catch (Exception e) {
            batchHelper.fail(e);
            batchMutations.clear();
        }
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        this.batchHelper = new BatchHelper(batchSize, collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
