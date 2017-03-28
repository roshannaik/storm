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
package org.apache.storm.executor;

import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutorTransfer implements JCQueue.Consumer {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorTransfer.class);

    private final WorkerState workerData;
    private final Map stormConf;
    private final KryoTupleSerializer serializer;
    private final boolean isDebug;
    private final int producerBatchSz;
    private int currBatchSz = 0;

    HashMap<Integer, List<AddressedTuple>> localMap = new HashMap<>();
    HashMap<Integer, List<TaskMessage>> remoteMap  = new HashMap<>();

    public ExecutorTransfer(WorkerState workerData, Map stormConf) {
        this.workerData = workerData;
        this.stormConf = stormConf;
        this.serializer = new KryoTupleSerializer(stormConf, workerData.getWorkerTopologyContext());
        this.isDebug = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
        this.producerBatchSz = Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE));
    }

    public void transfer(int task, Tuple tuple) {
        AddressedTuple addressedTuple = new AddressedTuple(task, tuple);
        if (isDebug) {
            LOG.info("TRANSFERRING tuple {}", addressedTuple);
        }
        accept(addressedTuple);
        ++currBatchSz;
        if(currBatchSz>=producerBatchSz) {
            flush(); // TODO: Roshan: flush needs to be called on timeout also
            currBatchSz=0;
        }
    }

    public String getName() {
        return "No Queue here";
    }

    @Override
    public void accept(Object event)  { //TODO: change param type to AddresedTuple ?
        workerData.classifyLocalOrRemote(serializer, (AddressedTuple) event, localMap, remoteMap);
    }

    @Override
    public void flush() {
        if (!localMap.isEmpty()) {
            workerData.transferLocal(localMap);
        }
        if (!remoteMap.isEmpty()) {
            workerData.transferRemote(remoteMap);
        }
        localMap.clear();
        remoteMap.clear();
    }

}
