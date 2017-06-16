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

package org.apache.storm.utils;

// TODO: publish is blocking(), either we need a timeout on it. or need to figure out if timeouts are needed for stopping.
// TODO: Remove return 1L in Worker.java
// TODO: Add metric to count how often consumeBatch consume nothing
// TODO: shutdown takes longer (in IDE) due to ZK connection termination

import com.lmax.disruptor.dsl.ProducerType;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.metric.internal.RateTracker;
import org.jctools.queues.ConcurrentCircularArrayQueue;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;


public class JCQueue implements IStatefulObject {

    public enum ProducerKind { SINGLE, MULTI };

    private static final Logger LOG = LoggerFactory.getLogger(JCQueue.class);
    private static final Object INTERRUPT = new Object();
//    private static final String PREFIX = "events-";
    private ThroughputMeter emptyMeter = new ThroughputMeter("EmptyBatch", 5_000_000);

    private interface Inserter {
        // blocking call that can be interrupted with Thread.interrupt()
        void add(Object obj) throws InterruptedException ;
    }

    private class DirectInserter implements Inserter {

        public DirectInserter() {
        }

        /** Blocking call, that can be interrupted via Thread.interrupt */
        @Override
        public void add(Object obj) throws InterruptedException {
            boolean inserted = publishDirectSingle(obj);
            while(!inserted) {
                sleep(0,1);
                inserted = publishDirectSingle(obj);
            }
        }
    }

    private void sleep(int millis, int nanos) throws InterruptedException {
//        Thread.yield();
        Thread.sleep(millis,nanos);
    }

    private class BatchInserter implements Inserter {
        private ArrayList<Object> _currentBatch;
        private ThroughputMeter fullMeter = new ThroughputMeter("Q Full", 100_000_000);

        public BatchInserter() {
            _currentBatch = new ArrayList<>(_inputBatchSize);
        }

        @Override
        public void add(Object obj) throws InterruptedException {
            _currentBatch.add(obj);

            if (_currentBatch.size() >= _inputBatchSize) {
                flush();
            }
        }

        // Does not return till at least 1 element is drained or Thread.interrupt() is received
        private void flush( ) throws InterruptedException {
            int publishCount = publishDirect(_currentBatch);
            while (publishCount==0) { // retry till at least 1 element is drained
                fullMeter.record();
                if (Thread.currentThread().isInterrupted())
                    return;
                sleep(1000, 0);
                publishCount = publishDirect(_currentBatch);
            }
            _currentBatch.subList(0, publishCount).clear();
        }

    } // class BatchInserter

    /**
     * This inner class provides methods to access the metrics of the disruptor queue.
     */
    public class QueueMetrics {
        private final RateTracker _rateTracker = new RateTracker(10000, 10);

        private final RunningStat drainCount =  new RunningStat("drainCount", 10_000_000, true);

        public long writePos() {
            return _buffer.size();
        }

        public long readPos() {
            return 0L;
        }

        public long overflow() {
            return 0;
        }

        public long population() {
            return _buffer.size();
        }

        public long capacity() {
            return _buffer.capacity();
        }

        public float pctFull() {
            return (1.0F * population() / capacity());
        }

        public Object getState() {
            Map state = new HashMap<String, Object>();

            // get readPos then writePos so it's never an under-estimate
            long rp = readPos();
            long wp = writePos();

            final double arrivalRateInSecs = _rateTracker.reportRate();

            //Assume the queue is stable, in which the arrival rate is equal to the consumption rate.
            // If this assumption does not hold, the calculation of sojourn time should also consider
            // departure rate according to Queuing Theory.
            final double sojournTime = (wp - rp) / Math.max(arrivalRateInSecs, 0.00001) * 1000.0;

            state.put("capacity", capacity());
            state.put("population", wp - rp);
            state.put("write_pos", wp); // TODO: Roshan: eliminate these *_pos ?
            state.put("read_pos", rp);
            state.put("arrival_rate_secs", arrivalRateInSecs);
            state.put("sojourn_time_ms", sojournTime); //element sojourn time in milliseconds
            state.put("overflow", 0);
            state.put("drainCountAvg", drainCount.mean());
            return state;
        }

        public void notifyArrivals(long counts) {
            _rateTracker.notify(counts);
        }

        public void notifyDrain(int count) {
            drainCount.push(count);
        }

        public void close() {
            _rateTracker.close();
        }

    }

    private final ConcurrentCircularArrayQueue<Object> _buffer;

    private final int _inputBatchSize;
    private final ConcurrentHashMap<Long, Inserter> _batchers = new ConcurrentHashMap<>();
    private final JCQueue.QueueMetrics _metrics;

    private String _queueName = "";
    private long _readTimeout;
    private int _highWaterMark = 0;
    private int _lowWaterMark = 0;

//    private final AtomicLong _overflowCount = new AtomicLong(0);

    public JCQueue(String queueName, ProducerType type, int size, long readTimeout, int inputBatchSize) {
        this(queueName, type==ProducerType.SINGLE ? ProducerKind.SINGLE : ProducerKind.MULTI, size, readTimeout, inputBatchSize);
    }

    public JCQueue(String queueName,  int size, long readTimeout, int inputBatchSize) {
        this(queueName, ProducerType.MULTI, size, readTimeout, inputBatchSize);
    }

    public JCQueue(String queueName, ProducerKind type, int size, long readTimeout, int inputBatchSize) {
        this._queueName = queueName;
        _readTimeout = readTimeout;

        if (type == ProducerKind.SINGLE)
            _buffer = new SpscArrayQueue<>(size);
        else
            _buffer = new MpscArrayQueue<>(size);

        _metrics = new JCQueue.QueueMetrics();
        //The batch size can be no larger than half the full queue size.
        //This is mostly to avoid contention issues.
        _inputBatchSize = Math.max(1, Math.min(inputBatchSize, size/2));
    }

    public String getName() {
        return _queueName;
    }

    public boolean isFull() {
        return (_metrics.population() + 0) >= _metrics.capacity();
    }

    public void haltWithInterrupt() {
        if( publishDirectSingle(INTERRUPT) ){
            _metrics.close();
        } else {
            throw new RuntimeException(new QueueFullException());
        }
    }

    public int consumeBatchWhenAvailable(JCQueue.Consumer handler) {
        try {
            return consumeBatchToCursor(handler);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private int consumeBatchToCursor(Consumer consumer) throws InterruptedException {
        int count = _buffer.drain(
                obj -> {
                    try {
                        if (obj == INTERRUPT) {
                            throw new InterruptedException("JCQ processing interrupted");
                        }
                        consumer.accept(obj);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        consumer.flush();
        _metrics.notifyDrain(count);
        if(count==0)
            emptyMeter.record();
        return count;
    }


    private static Long getId() {
        return Thread.currentThread().getId();
    }

    // Non Blocking. returns true/false indicating success/failure
    private boolean publishDirectSingle(Object obj)  {
        if( _buffer.offer(obj) ) {
            _metrics.notifyArrivals(1);
            return true;
        }
        return false;
    }

    // Non Blocking. returns count of how many inserts succeeded
    private int publishDirect(ArrayList<Object> objs) {
        MessagePassingQueue.Supplier<Object> supplier =
                new MessagePassingQueue.Supplier<Object> (){
                    int i = 0;
                    @Override
                    public Object get() {
                        return objs.get(i++);
                    }
                };
        int count = _buffer.fill(supplier, objs.size());
        _metrics.notifyArrivals(count);
        return count;
    }

    /** Blocking call, that can be interrupted via Thread.interrupt() */
    public void publish(Object obj) {
        Long id = getId();
        Inserter batcher = _batchers.get(id);
        if (batcher == null) {
            if (_inputBatchSize > 1) {
                batcher = new BatchInserter();
            } else {
                batcher = new DirectInserter();
            }
            //This publisher thread is the only one ever creating a batcher for this thd id, so this is safe
            _batchers.put(id, batcher);
        }
        try {
            batcher.add(obj);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Object getState() {
        return _metrics.getState();
    }


    //This method enables the metrics to be accessed from outside of the JCQueue class
    public JCQueue.QueueMetrics getMetrics() {
        return _metrics;
    }

    private class QueueFullException extends Exception {
        public QueueFullException() {
            super(_queueName + " is full");
        }
    }

    public interface Consumer {
        void accept(Object event) throws Exception ;
        void flush();
    }
}