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

package org.apache.storm.daemon.worker;

import org.apache.storm.Config;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.TransferDrainer;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.SmartThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Transfers messages destined to other workers
class WorkerTransfer implements JCQueue.Consumer {
    static final Logger LOG = LoggerFactory.getLogger(WorkerTransfer.class);

    private final TransferDrainer drainer;
    private WorkerState workerState;
    private final KryoTupleSerializer serializer;
    private IWaitStrategy backPressureWaitStrategy;

    JCQueue transferQueue; // [remoteTaskId] -> JCQueue. Some entries maybe null (if no emits to those tasksIds from this worker)
    AtomicBoolean[] remoteBackPressureStatus; // [[remoteTaskId] -> true/false : indicates if remote task is under BP.

    public WorkerTransfer(WorkerState workerState, Map<String, Object> topologyConf, int maxTaskIdInTopo) {
        this.workerState = workerState;
        this.serializer = new KryoTupleSerializer(topologyConf, workerState.getWorkerTopologyContext());
        this.backPressureWaitStrategy = IWaitStrategy.createBackPressureWaitStrategy(topologyConf);
        this.drainer = new TransferDrainer();
        this.remoteBackPressureStatus = new AtomicBoolean[maxTaskIdInTopo+1];
        for (int i = 0; i < remoteBackPressureStatus.length; i++) {
            remoteBackPressureStatus[i] = new AtomicBoolean(false);
        }

        Integer xferQueueSz = ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
        Integer xferBatchSz = ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_TRANSFER_BATCH_SIZE));
        if (xferBatchSz > xferQueueSz / 2) {
            throw new IllegalArgumentException(Config.TOPOLOGY_TRANSFER_BATCH_SIZE + ":" + xferBatchSz + " must be no more than half of "
                + Config.TOPOLOGY_TRANSFER_BUFFER_SIZE + ":" + xferQueueSz);
        }

        this.transferQueue = new JCQueue("worker-transfer-queue", xferQueueSz, 0, xferBatchSz, backPressureWaitStrategy);
    }

    public JCQueue getTransferQueue() {
        return transferQueue;
    }

    public SmartThread makeTransferThread() {
        return Utils.asyncLoop(() -> {
            if (transferQueue.consume(this) == 0) {
                return 1L;
            }
            return 0L;
        });
    }

    @Override
    public void accept(Object tuple) {
        AddressedTuple addressedTuple = (AddressedTuple) tuple;
        TaskMessage tm = new TaskMessage(addressedTuple.getDest(), serializer.serialize(addressedTuple.getTuple()));
        drainer.add(tm);
    }

    @Override
    public void flush() throws InterruptedException {
        ReentrantReadWriteLock.ReadLock readLock = workerState.endpointSocketLock.readLock();
        try {
            readLock.lock();
            drainer.send(workerState.cachedTaskToNodePort.get(), workerState.cachedNodeToPortSocket.get());
        } finally {
            readLock.unlock();
        }
        drainer.clear();
    }

    /* Not a Blocking call. If cannot emit, will add 'tuple' to pendingEmits and return 'false'. 'pendingEmits' can be null */
    public boolean tryTransferRemote(AddressedTuple tuple, Queue<AddressedTuple> pendingEmits) {
        if (!remoteBackPressureStatus[tuple.dest].get()) {
            if (transferQueue.tryPublish(tuple)) {
                return true;
            }
        } else {
            LOG.debug("Noticed Back Pressure in remote task {}", tuple.dest);
        }
        if (pendingEmits != null) {
            pendingEmits.add(tuple);
        }
        return false;
    }

    public void flushRemotes() throws InterruptedException {
        transferQueue.flush();
    }

    public boolean tryFlushRemotes() {
        return transferQueue.tryFlush();
    }


    public void haltTransferThd() {
        transferQueue.haltWithInterrupt();
    }

}