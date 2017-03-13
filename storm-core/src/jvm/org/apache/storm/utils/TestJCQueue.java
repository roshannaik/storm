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

import org.jctools.queues.MessagePassingQueue;

import java.util.concurrent.locks.LockSupport;

public class TestJCQueue {

    public static void main(String[] args) throws Exception {

        JCQueue q = new JCQueue("q1", JCQueue.ProducerKind.MULTI, 1024, 100, 100, 0);

        final Producer prod1 = new Producer(q);
        final Consumer cons1 = new Consumer(q);

        cons1.start();
        prod1.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.err.println("Stopping");
                prod1.interrupt();
                cons1.interrupt();
                prod1.join();
                cons1.join();
                System.err.printf("Producer : %d,  Throughput: %,d \n", prod1.count, prod1.throughput() );
                System.err.printf("Consumer : %d,  Throughput: %,d \n", cons1.counter.get(), prod1.throughput() );
            } catch (InterruptedException e) {
                return;
            }
        }));

        while(true)
            Thread.sleep(1000);


    }
}


class Consumer extends Thread {
    private final JCQueue q;
    public final MutableLong counter = new MutableLong(0);
    long runTime = 0;
    public Consumer(JCQueue q) {
        super("Consumer");
        this.q = q;
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) throws Exception {
            counter.increment();
            if(counter.get()==1000_000) {
//                System.err.println( System.currentTimeMillis() );
                counter.set(0);
            }
        }

        @Override
        public void flush() {
            // no-op
        }
    }

    @Override
    public void run() {
        Handler handler = new Handler();
        long start = System.currentTimeMillis();
        while(!Thread.interrupted()) {
            int x = q.consumeBatch(handler);
            if(x==0)
                LockSupport.parkNanos(1);
        }
        runTime = System.currentTimeMillis() - start;
    }

    public long throughput() {
        return counter.get() / (runTime / 1000);
    }
}


class Producer extends Thread {
    private final JCQueue q;
    public long count=0;
    public long runTime = 0;
    public Producer(JCQueue q) {
        super("Producer");
        this.q = q;
    }

    public long throughput() {
        return count / (runTime / 1000);
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            q.publish(++count);
        }
        runTime = System.currentTimeMillis() - start;

    }
}