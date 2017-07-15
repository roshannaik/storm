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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutorTransfer  {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorTransfer.class);

    private final WorkerState workerData; // TODO: Roshan: consider having a local copy of relevant info ? and update it when it changes
    private final KryoTupleSerializer serializer;
    private final boolean isDebug;
    private final int producerBatchSz;
    private int remotesBatchSz = 0;
    private final ArrayList<JCQueue> shortExecutorReceiveQueueMap; // using arrays instead of hashmap for perf reasons
    private final ArrayList<JCQueue> outboundQueues;

    final HashMap<Integer, List<TaskMessage>> remoteMap  = new HashMap<>();

    public ExecutorTransfer(WorkerState workerData, Map stormConf) {
        this.workerData = workerData;
        this.serializer = new KryoTupleSerializer(stormConf, workerData.getWorkerTopologyContext());
        this.isDebug = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
        this.producerBatchSz = Utils.getInt(stormConf.get(Config.TOPOLOGY_PRODUCER_BATCH_SIZE));
        this.shortExecutorReceiveQueueMap = Utils.convertToArray(workerData.getShortExecutorReceiveQueueMap());
        this.outboundQueues = new ArrayList<JCQueue>(Collections.nCopies(shortExecutorReceiveQueueMap.size(), null) );
    }


    public void transfer(int task, Tuple tuple) throws InterruptedException {
        AddressedTuple addressedTuple = new AddressedTuple(task, tuple);
        if (isDebug) {
            LOG.info("TRANSFERRING tuple {}", addressedTuple);
        }

        boolean isLocal = transferLocal(addressedTuple);
        if (!isLocal) {
            cacheRemoteTuples(serializer, addressedTuple, remoteMap);
            ++remotesBatchSz;
            if(remotesBatchSz >=producerBatchSz) {
                flushRemotes();
                remotesBatchSz =0;
            }
        }
    }

    public String getName() {
        return "No Queue here";
    }

    //TODO: Roshan: double batching going on here. do we need it ? remote map can be created in the workerTransferThd
    private static void cacheRemoteTuples(KryoTupleSerializer serializer, AddressedTuple tuple, Map<Integer, List<TaskMessage>> remoteMap) {
        int destTask = tuple.dest;
        if (! remoteMap.containsKey(destTask)) {
            remoteMap.put(destTask, new ArrayList<>());
        }
        remoteMap.get(destTask).add(new TaskMessage(destTask, serializer.serialize(tuple.getTuple())));
    }

    // flushes local and remote messages
    public void flush() throws InterruptedException {
        flushLocal();
        flushRemotes();
    }

    private void flushLocal() throws InterruptedException {
        for (int i = 0; i < shortExecutorReceiveQueueMap.size(); i++) {
            JCQueue q = shortExecutorReceiveQueueMap.get(i);
            if(q!=null)
                q.flush();
        }
    }

    private void flushRemotes() throws InterruptedException {
        if (!remoteMap.isEmpty()) {
            workerData.flushRemotes(remoteMap);
            remoteMap.clear();
        }
    }

    public boolean transferLocal(AddressedTuple tuple) throws InterruptedException {
        workerData.checkSerialize(serializer, tuple);
        JCQueue queue = shortExecutorReceiveQueueMap.get(tuple.dest);
        if (queue==null)
            return false;
        queue.publish(tuple);
        outboundQueues.set(tuple.dest, queue);
        return true;
    }

}
