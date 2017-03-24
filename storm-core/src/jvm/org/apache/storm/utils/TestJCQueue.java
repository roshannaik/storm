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

import java.util.concurrent.locks.LockSupport;

public class TestJCQueue {

    public static void main(String[] args) throws Exception {
//        oneProducer1Consumer();
//        oneProducer2Consumers();
        producerFwdConsumer();

//        JCQueue spoutQ = new JCQueue("spoutQ", JCQueue.ProducerKind.MULTI, 1024, 100, 100, 0);
//        JCQueue ackQ = new JCQueue("ackQ", JCQueue.ProducerKind.MULTI, 1024, 100, 100, 0);
//
//        final AckingProducer ackingProducer = new AckingProducer(spoutQ, ackQ);
//        final Acker acker = new Acker(ackQ, spoutQ);
//
//        runAllThds(ackingProducer, acker);

        while(true)
            Thread.sleep(1000);

    }

    private static void producerFwdConsumer() {
        JCQueue q1 = new JCQueue("q1", JCQueue.ProducerKind.MULTI, 1024, 100, 100, 0);
        JCQueue q2 = new JCQueue("q2", JCQueue.ProducerKind.MULTI, 1024, 100, 100, 0);

        final Producer prod = new Producer(q1);
        final Forwarder fwd = new Forwarder(q1,q2);
        final Consumer cons = new Consumer(q2);

        runAllThds(prod, fwd, cons);
    }


    private static void oneProducer1Consumer() {
        JCQueue q1 = new JCQueue("q1", JCQueue.ProducerKind.MULTI, 1024, 100, 100, 0);

        final Producer prod1 = new Producer(q1);
        final Consumer cons1 = new Consumer(q1);

        runAllThds(prod1, cons1);
    }

    private static void oneProducer2Consumers() {
        JCQueue q1 = new JCQueue("q1", JCQueue.ProducerKind.MULTI, 1024, 100, 100, 0);
        JCQueue q2 = new JCQueue("q2", JCQueue.ProducerKind.MULTI, 1024, 100, 100, 0);

        final Producer2 prod1 = new Producer2(q1,q2);
        final Consumer cons1 = new Consumer(q1);
        final Consumer cons2 = new Consumer(q2);

        runAllThds(prod1, cons1, cons2);
    }

    public static void runAllThds(MyThread... threads) {
        for (Thread thread : threads) {
            thread.start();
        }
        addShutdownHooks(threads);
    }

    public static void addShutdownHooks(MyThread... threads) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.err.println("Stopping");
                for (Thread thread : threads) {
                    thread.interrupt();
                }

                for (Thread thread : threads) {
                    System.err.println("Waiting for " + thread.getName());
                    thread.join();
                }

                for (MyThread thread : threads) {
                    System.err.printf("%s : %d,  Throughput: %,d \n", thread.getName(), thread.count, thread.throughput() );
                }
            } catch (InterruptedException e) {
                return;
            }
        }));

    }

}



abstract class MyThread extends Thread  {
    public long count=0;
    public long runTime = 0;

    public MyThread(String thdName) {
        super(thdName);
    }

    public long throughput() {
        return getCount() / (runTime / 1000);
    }
    public long getCount() { return  count; }
}

class Producer extends MyThread {
    private final JCQueue q;
    public Producer(JCQueue q) {
        super("Producer");
        this.q = q;
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

// writes to two queues
class Producer2 extends MyThread {
    private final JCQueue q1;
    private final JCQueue q2;

    public Producer2(JCQueue q1, JCQueue q2) {
        super("Producer2");
        this.q1 = q1;
        this.q2 = q2;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            q1.publish(++count);
            q2.publish(count);
        }
        runTime = System.currentTimeMillis() - start;

    }
}


// writes to two queues
class AckingProducer extends MyThread {
    private final JCQueue ackerInQ;
    private final JCQueue spoutInQ;

    public AckingProducer(JCQueue ackerInQ, JCQueue spoutInQ) {
        super("AckingProducer");
        this.ackerInQ = ackerInQ;
        this.spoutInQ = spoutInQ;
    }

    @Override
    public void run() {
        Handler handler = new Handler();
        long start = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            int x = spoutInQ.consumeBatch(handler);
            ackerInQ.publish(count);
        }
        runTime = System.currentTimeMillis() - start;
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) throws Exception {
        }

        @Override
        public void flush() {
            // no-op
        }
    }
}

// reads from ackerInQ and writes to spout queue
class Acker extends MyThread {
    private final JCQueue ackerInQ;
    private final JCQueue spoutInQ;

    public Acker(JCQueue ackerInQ, JCQueue spoutInQ) {
        super("Acker");
        this.ackerInQ = ackerInQ;
        this.spoutInQ = spoutInQ;
    }


    @Override
    public void run() {
        long start = System.currentTimeMillis();
        Handler handler = new Handler();
        while (!Thread.interrupted()) {
            ackerInQ.consumeBatch(handler);
        }
        runTime = System.currentTimeMillis() - start;
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) throws Exception {
            spoutInQ.publish(event);
        }

        @Override
        public void flush() {
            // no-op
        }
    }
}

class Consumer extends MyThread {
    private final JCQueue q;
    public final MutableLong counter = new MutableLong(0);
    public Consumer(JCQueue q) {
        super("Consumer");
        this.q = q;
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) throws Exception {
            counter.increment();
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

    @Override
    public long getCount() { return  counter.get(); }
}



class Forwarder extends MyThread {
    private final JCQueue inq;
    private final JCQueue outq;
    public final MutableLong counter = new MutableLong(0);

    public Forwarder(JCQueue inq, JCQueue outq) {
        super("Forwarder");
        this.inq = inq;
        this.outq = outq;
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) throws Exception {
            outq.publish(event);
            counter.increment();
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
            int x = inq.consumeBatch(handler);
            if(x==0)
                LockSupport.parkNanos(1);
        }
        runTime = System.currentTimeMillis() - start;
    }

    @Override
    public long getCount() { return counter.get(); }
}