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

// TODO : Support wait/sleep strategy for both drain and fill side
// TODO: Remove return 1L in Worker.java

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

import java.util.concurrent.locks.LockSupport;


public class JCQueue implements IStatefulObject {

    public enum ProducerKind { SINGLE, MULTI };

    private static final Logger LOG = LoggerFactory.getLogger(JCQueue.class);
    private static final Object INTERRUPT = new Object();
    private static final String PREFIX = "disruptor-";
//    private static final JCQueue.FlusherPool FLUSHER = new JCQueue.FlusherPool();
    private ThroughputMeter emptyMeter = new ThroughputMeter("EmptyBatch", 5_000_000);

    private interface ThreadLocalInserter {
        public void add(Object obj);
    }

    private class ThreadLocalJustInserter implements JCQueue.ThreadLocalInserter {
//        private final ConcurrentLinkedQueue<Object> _overflow = new ConcurrentLinkedQueue<>();;

        public ThreadLocalJustInserter() {
        }

        //called by the main thread and should not block for an undefined period of time
        public void add(Object obj) {
            boolean inserted = publishDirectSingle(obj);
            while(!inserted) {
                LockSupport.parkNanos(1);
                if(Thread.interrupted())
                    return;
                inserted = publishDirectSingle(obj);

            }
        }
    }

    private class ThreadLocalBatcher implements ThreadLocalInserter {
//        private final ConcurrentLinkedQueue<ArrayList<Object>> _overflow;
        private ArrayList<Object> _currentBatch;
        private ThroughputMeter fullMeter = new ThroughputMeter("Q Full", 10_000_000);

        public ThreadLocalBatcher() {
//            _overflow = new ConcurrentLinkedQueue<ArrayList<Object>>();
            _currentBatch = new ArrayList<>(_inputBatchSize);
        }

        public void add(Object obj) {
            _currentBatch.add(obj);

            if (_currentBatch.size() >= _inputBatchSize) {
                flush();
            }
        }

        // Does not return till completely flushed or Thread.interrupted()
        private void flush( ) {
            int publishCount = publishDirect(_currentBatch);
            _currentBatch.subList(0, publishCount).clear();
            while (!_currentBatch.isEmpty()) { // retry
                LockSupport.parkNanos(1);
                if(Thread.interrupted())
                    return;
                publishCount = publishDirect(_currentBatch);
                if (publishCount==0)
                    fullMeter.record();
                else
                    _currentBatch.subList(0, publishCount).clear();
            }
        }

    } // class ThreadLocalBatcher

    /**
     * This inner class provides methods to access the metrics of the disruptor queue.
     */
    public class QueueMetrics {
        private final RateTracker _rateTracker = new RateTracker(10000, 10);

        private final RunningStat drainCount =  new RunningStat("drainCount", 10_000_000, true);
        private final RunningStat publishCount =  new RunningStat("publishCount", 10_000_000, true);

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
            state.put("write_pos", wp);
            state.put("read_pos", rp);
            state.put("arrival_rate_secs", arrivalRateInSecs);
            state.put("sojourn_time_ms", sojournTime); //element sojourn time in milliseconds
            state.put("overflow", 0);
            state.put("drainCountAvg", drainCount.mean());
            state.put("publishCountAvg", publishCount.mean());
            return state;
        }

        public void notifyArrivals(long counts) {
            _rateTracker.notify(counts);
        }

        public void notifyDrain(int count) {
            drainCount.push(count);
        }

        public void notifyPublish(int count) {
            publishCount.push(count);
        }

        public void close() {
            _rateTracker.close();
        }

    }

    private final ConcurrentCircularArrayQueue<Object> _buffer;

    private final int _inputBatchSize;
    private final ConcurrentHashMap<Long, JCQueue.ThreadLocalInserter> _batchers = new ConcurrentHashMap<>();
//    private final JCQueue.Flusher _flusher;
    private final JCQueue.QueueMetrics _metrics;

    private String _queueName = "";
    private long _readTimeout;
    private int _highWaterMark = 0;
    private int _lowWaterMark = 0;

//    private final AtomicLong _overflowCount = new AtomicLong(0);

    public JCQueue(String queueName, ProducerType type, int size, long readTimeout, int inputBatchSize, long flushInterval) {
        this(queueName, type==ProducerType.SINGLE ? ProducerKind.SINGLE : ProducerKind.MULTI, size, readTimeout, inputBatchSize, flushInterval);
    }

    public JCQueue(String queueName,  int size, long readTimeout, int inputBatchSize, long flushInterval) {
        this(queueName, ProducerType.MULTI, size, readTimeout, inputBatchSize, flushInterval);
    }

    public JCQueue(String queueName, ProducerKind type, int size, long readTimeout, int inputBatchSize, long flushInterval) {
        this._queueName = PREFIX + queueName;
        _readTimeout = readTimeout;

        if (type == ProducerKind.SINGLE)
            _buffer = new SpscArrayQueue<>(size);
        else
            _buffer = new MpscArrayQueue<>(size);

        _metrics = new JCQueue.QueueMetrics();
        //The batch size can be no larger than half the full queue size.
        //This is mostly to avoid contention issues.
        _inputBatchSize = Math.max(1, Math.min(inputBatchSize, size/2));

//        _flusher = new JCQueue.Flusher(Math.max(flushInterval, 5), _queueName);
//        _flusher.start();
    }

    public String getName() {
        return _queueName;
    }

    public boolean isFull() {
        return (_metrics.population() + 0) >= _metrics.capacity();
    }

    public void haltWithInterrupt() {
        if( publishDirectSingle(INTERRUPT) ){
//                _flusher.close();
            _metrics.close();
        } else {
            throw new RuntimeException(new QueueFullException());
        }
    }

    public int consumeBatch(JCQueue.Consumer handler) {
//        if (_metrics.population() > 0) {
            return consumeBatchWhenAvailable(handler);
//        }
//        return 0;
    }

    public int consumeBatchWhenAvailable(Consumer handler) {
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


    public void registerBackpressureCallback(DisruptorBackpressureCallback cb) {
        // NO-OP
    }

    private static Long getId() {
        return Thread.currentThread().getId();
    }

    private boolean publishDirectSingle(Object obj)  {
        if( _buffer.offer(obj) ) {
            _metrics.notifyArrivals(1);
            _metrics.notifyPublish(1);
            return true;
        }
        return false;
    }

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
        _metrics.notifyPublish(count);
        return count;
    }

    public void publish(Object obj) {
        Long id = getId();
        JCQueue.ThreadLocalInserter batcher = _batchers.get(id);
        if (batcher == null) {
            if (_inputBatchSize > 1) {
                batcher = new ThreadLocalBatcher();
            } else {
                batcher = new ThreadLocalJustInserter();
            }
            //This thread is the only one ever creating a batcher for this thd id, so this is safe
            _batchers.put(id, batcher);
        }
        batcher.add(obj);
    }

    @Override
    public Object getState() {
        return _metrics.getState();
    }

    public JCQueue setHighWaterMark(double highWaterMark) {
        this._highWaterMark = (int)(_metrics.capacity() * highWaterMark);
        return this;
    }

    public JCQueue setLowWaterMark(double lowWaterMark) {
        this._lowWaterMark = (int)(_metrics.capacity() * lowWaterMark);
        return this;
    }

    public int getHighWaterMark() {
        return this._highWaterMark;
    }

    public int getLowWaterMark() {
        return this._lowWaterMark;
    }

    public JCQueue setEnableBackpressure(boolean enableBackpressure) {
        // NOOP
        return this;
    }

    //This method enables the metrics to be accessed from outside of the JCQueue class
    public JCQueue.QueueMetrics getMetrics() {
        return _metrics;
    }

    public boolean getThrottleOn() {
        return false;
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