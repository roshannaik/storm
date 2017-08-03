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

package org.apache.storm.policy;

import org.apache.storm.Config;
import org.apache.storm.utils.ObjectReader;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public class WaitStrategyPark implements IWaitStrategy {
    private long parkTimeNanoSec;

    @Override
    public void prepare(Map<String, Object> conf) {
        long microsec = ObjectReader.getLong(conf.get(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC));
        parkTimeNanoSec = microsec * 1_000;
    }

    public WaitStrategyPark() { // required for instantiation via reflection. call prepare() thereafter
    }

    // Convenience alternative to prepare() for use in Tests
    public WaitStrategyPark(long microsec) {
        parkTimeNanoSec = microsec * 1_000;
    }



    @Override
    public int idle(int idleCounter) throws InterruptedException {
        if(idleCounter==0)
            return 0;
        LockSupport.parkNanos(parkTimeNanoSec);
        return idleCounter+1;
    }
}
