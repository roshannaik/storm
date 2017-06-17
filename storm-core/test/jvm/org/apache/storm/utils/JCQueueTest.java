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
package org.apache.storm.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Assert;
import org.junit.Test;
import junit.framework.TestCase;

public class JCQueueTest extends TestCase {

    private final static int TIMEOUT = 5000; // MS
    private final static int PRODUCER_NUM = 4;

    @Test
    public void testFirstMessageFirst() throws InterruptedException {
      for (int i = 0; i < 100; i++) {
        JCQueue queue = createQueue("firstMessageOrder", 16);

        queue.publish("FIRST");

        Runnable producer = new IncProducer(queue, i+100);

        final AtomicReference<Object> result = new AtomicReference<>();
          Runnable consumer = new ConsumerThd(queue, new JCQueue.Consumer() {
            private boolean head = true;

            @Override
            public void accept(Object obj)
                    throws Exception {
                if (head) {
                    head = false;
                    result.set(obj);
                }
            }

            @Override
            public void flush() {
                return;
            }
          });

        run(producer, consumer, queue);
        Assert.assertEquals("We expect to receive first published message first, but received " + result.get(),
                "FIRST", result.get());
      }
    }
   
    @Test 
    public void testInOrder() throws InterruptedException {
        final AtomicBoolean allInOrder = new AtomicBoolean(true);

        JCQueue queue = createQueue("consumerHang", 1024);
        Runnable producer = new IncProducer(queue, 1024*1024);
        Runnable consumer = new ConsumerThd(queue, new JCQueue.Consumer() {
            long _expected = 0;
            @Override
            public void accept(Object obj)  throws Exception {
                if (_expected != ((Number) obj).longValue()) {
                    allInOrder.set(false);
                    System.out.println("Expected " + _expected + " but got " + obj);
                }
                _expected++;
            }

            @Override
            public void flush() {
                return;
            }
        } ) ;
        run(producer, consumer, queue, 1000, 1);
        Assert.assertTrue("Messages delivered out of order",
                allInOrder.get());
    }

    @Test 
    public void testInOrderBatch() throws InterruptedException {
        final AtomicBoolean allInOrder = new AtomicBoolean(true);

        JCQueue queue = createQueue("consumerHang", 10, 1024);
        Runnable producer = new IncProducer(queue, 1024*1024);
        Runnable consumer = new ConsumerThd(queue, new JCQueue.Consumer() {
            long _expected = 0;
            @Override
            public void accept(Object obj)
                    throws Exception {
                if (_expected != ((Number)obj).longValue()) {
                    allInOrder.set(false);
                    System.out.println("Expected "+_expected+" but got "+obj);
                }
                _expected++;
            }

            @Override
            public void flush() {
                return;
            }
        });

        run(producer, consumer, queue, 1000, 1);
        Assert.assertTrue("Messages delivered out of order",
                allInOrder.get());
    }


    private void run(Runnable producer, Runnable consumer, JCQueue queue)
            throws InterruptedException {
        run(producer, consumer, queue, 10, PRODUCER_NUM);
    }

    private void run(Runnable producer, Runnable consumer, JCQueue queue, int sleepMs, int producerNum)
            throws InterruptedException {

        Thread[] producerThreads = new Thread[producerNum];
        for (int i = 0; i < producerNum; i++) {
            producerThreads[i] = new Thread(producer);
            producerThreads[i].start();
        }
        
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        Thread.sleep(sleepMs);
        for (int i = 0; i < producerNum; i++) {
            producerThreads[i].interrupt();
        }
        for (int i = 0; i < producerNum; i++) {
            producerThreads[i].join(TIMEOUT);
            assertFalse("producer "+i+" is still alive", producerThreads[i].isAlive());
        }
        queue.haltWithInterrupt();
        consumerThread.join(TIMEOUT);
        assertFalse("consumer is still alive", consumerThread.isAlive());
    }

    private static class IncProducer implements Runnable {
        private JCQueue queue;
        private long _max;

        IncProducer(JCQueue queue, long max) {
            this.queue = queue;
            this._max = max;
        }

        @Override
        public void run() {
            for (long i = 0; i < _max && !(Thread.currentThread().isInterrupted()); i++) {
                queue.publish(i);
            }
        }
    }

    private static class ConsumerThd implements Runnable {
        private JCQueue.Consumer handler;
        private JCQueue queue;

        ConsumerThd(JCQueue queue, JCQueue.Consumer handler) {
            this.handler = handler;
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                while(true) {
                    queue.consume(handler);
                }
            } catch(RuntimeException e) {
                //break
            }
        }
    }

    private static JCQueue createQueue(String name, int queueSize) {
        return new JCQueue(name, ProducerType.MULTI, queueSize, 0L, 1);
    }

    private static JCQueue createQueue(String name, int batchSize, int queueSize) {
        return new JCQueue(name, ProducerType.MULTI, queueSize, 0L, batchSize);
    }
}
