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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputMeter {

    private String name;
    private long history=0;
    private long startTime = 0;
    private int count;
    private long endTime = 0;
    private int times=0;
//    private Logger LOG;
    private int printFreq;
    private boolean disable;

    public ThroughputMeter(String name, int printFreq, boolean disable) {
        this.name = name;
//        LOG = LoggerFactory.getLogger(name);
        this.printFreq = printFreq;
        this.disable = disable;
        this.startTime = System.currentTimeMillis();
    }

    public ThroughputMeter(String name, int printFreq) {
        this(name, printFreq, false);
    }

    public void record() {
        ++count;
        ++history;
        if(++times==printFreq) {
            times=0;
            printCurrentThroughput();
        }
    }

    public long stop() {
        if(startTime==0)
            return 0;
        if(endTime==0)
            this.endTime = System.currentTimeMillis();
        return throughput(count, startTime, endTime);
    }

    public long printCurrentThroughput() {
        if(disable)
            return 0;
        if(startTime==0)
            return 0;
        long currTime = (endTime==0) ? System.currentTimeMillis() : endTime ;

        long result = throughput(count, startTime, currTime) / 1000; // K/sec
//        LOG.error("====> {} : {} K/sec", name, result);
        System.err.printf("%s - %s : %,d K/sec\n", name, Thread.currentThread().getName(), result);
        startTime = currTime;
        count=0;
        return result;
    }

    /** @return  events/sec    */
    private static long throughput(long count, long startTime, long endTime) {
        long gap = (endTime-startTime);
        return (count / gap) * 1000;
    }

}
