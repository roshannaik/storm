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
    private long startTime = 0;
    private int count;
    private long endTime = 0;
    private int times=0;
    public static Logger LOG;
    private int printFreq;

    public ThroughputMeter(String name, int printFreq) {
        this.name = name;
        LOG = LoggerFactory.getLogger(name);
        this.printFreq = printFreq;
        this.startTime = System.currentTimeMillis();
    }

    public void record() {
        ++count;
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
        if(startTime==0)
            return 0;
        long currTime = (endTime==0) ? System.currentTimeMillis() : endTime ;

        long result = throughput(count, startTime, currTime) / 1000;
        LOG.error("====> {} : {} K/sec", name, result);
        System.err.printf("%s : %,d K/sec\n", name, result);
        startTime = currTime;
        count=0;
        return result;
    }

    /** @return  events/sec    */
    private static long throughput(int count, long startTime, long endTime) {
        long gap = (endTime-startTime);
        return count / (gap / 1000);
    }

}
