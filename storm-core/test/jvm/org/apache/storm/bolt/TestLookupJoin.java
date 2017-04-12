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

package org.apache.storm.bolt;

import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestLookupJoin {

    String[] adImpressionFields = {"id", "userId", "product"};

    Object[][] adImpressions = {
            {1, 21, "book" },
            {2, 22, "watch" },
            {3, 23, "chair" },
            {4, 24, "tv" },
            {5, 25, "watch" },
            {6, 26, "camera" },
            {7, 27, "book" },
            {8, 28, "tv" },
            {9, 29, "camera" },
            {10,30, "tv" } };

    String[] orderFields = {"id", "userId", "product", "price"};

    Object[][] orders = {
            {11, 21, "book"  , 71},
            {12, 22, "watch" , 330},
            {13, 23, "chair" , 500},
            {14, 29, "tv"    , 2000},
            {15, 30, "watch" , 400},
    };

    @Test
    public void testBasicCount() throws Exception {
        ArrayList<Tuple> orderStream = makeStream("orders", orderFields, orders);
        ArrayList<Tuple> adImpressionStream = makeStream("ads", adImpressionFields, adImpressions);

        LookupJoinBolt bolt = new LookupJoinBolt(LookupJoinBolt.Selector.STREAM)
                .dataStream("orders")
                .lookupStream("ads", new BaseWindowedBolt.Count(10))
                .joinOn(orderFields[1], adImpressionFields[1] )
                .select("orders:id,ads:userId,product,price");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);

        for (Tuple tuple : adImpressionStream) {
            bolt.execute(tuple);
        }
        for (Tuple tuple : orderStream) {
            bolt.execute(tuple);
        }

        printResults(collector);
        Assert.assertEquals( 5, collector.actualResults.size() );
    }

    @Test
    public void testBasicTimeRetention() throws Exception {
        ArrayList<Tuple> orderStream = makeStream("orders", orderFields, orders);
        ArrayList<Tuple> adImpressionStream = makeStream("ads", adImpressionFields, adImpressions);

        LookupJoinBolt bolt = new LookupJoinBolt(LookupJoinBolt.Selector.STREAM)
                .dataStream("orders")
                .lookupStream("ads", new BaseWindowedBolt.Duration(2, TimeUnit.SECONDS))
                .joinOn(orderFields[1], adImpressionFields[1] )
                .select("orders:id,ads:userId,product,price");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);

        for (Tuple tuple : adImpressionStream) {
            bolt.execute(tuple);
        }
        for (Tuple tuple : orderStream) {
            bolt.execute(tuple);
        }

        printResults(collector);
        Assert.assertEquals( 5, collector.actualResults.size() );
    }


    private static ArrayList<Tuple> makeStream(String streamName, String[] fieldNames, Object[][] data) {
        ArrayList<Tuple> result = new ArrayList<>();
        MockContext mockContext = new MockContext(fieldNames);

        for (Object[] record : data) {
            TupleImpl rec = new TupleImpl(mockContext, Arrays.asList(record), 0, streamName);
            result.add( rec );
        }

        return result;
    }

    private static void printResults(MockCollector collector) {
        int counter=0;
        for (List<Object> rec : collector.actualResults) {
            System.out.print(++counter +  ") ");
            for (Object field : rec) {
                System.out.print(field + ", ");
            }
            System.out.println("");
        }
    }


    static class MockCollector extends OutputCollector {
        public ArrayList<List<Object>> actualResults = new ArrayList<>();

        public MockCollector() {
            super(null);
        }

        @Override
        public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {
            actualResults.add(tuple);
            return null;
        }

        @Override
        public void ack(Tuple input) {
            // no-op
        }
    } // class MockCollector

    static class MockContext extends GeneralTopologyContext {

        private final Fields fields;

        public MockContext(String[] fieldNames) {
            super(null, null, null, null, null, null);
            this.fields = new Fields(fieldNames);
        }

        public String getComponentId(int taskId) {
            return "component";
        }

        public Fields getComponentOutputFields(String componentId, String streamId) {
            return fields;
        }

    }

}
