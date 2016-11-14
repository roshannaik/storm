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

package org.apache.storm.starter.bolt;

import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.TupleWindowImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class TestWindowedQueryBolt {
    String[] userFields = {"userId", "name", "city"};
    Object[][] users = {
            {1, "roshan", "san jose"},
            {2, "harsha", "santa clara"},
            {3, "siva",   "dublin" },
            {4, "hugo",   "san mateo" },
            {5, "suresh", "sunnyvale" },
            {6, "guru",   "palo alto" },
            {7, "arun",   "bengaluru"},
            {8, "satish", "mumbai"},
            {9, "mani",   "chennai"}
    };

    String[] orderFields = {"orderId", "userId", "itemId", "price"};

    Object[][] orders = {
            {11, 2, 21, 7},
            {12, 2, 22, 3},
            {13, 3, 23, 4},
            {14, 4, 24, 5},
            {15, 5, 25, 2},
            {16, 6, 26, 7},
            {17, 6, 27, 4},
            {18, 7, 28, 2},
            {19, 8, 29, 9}
    };


    @Test
    public void testTrivial() throws Exception {
        ArrayList<Tuple> orderStream = makeStream("orders", orderFields, orders);
        TupleWindow window = makeTupleWindow(orderStream);

        WindowedQueryBolt bolt = new WindowedQueryBolt(WindowedQueryBolt.StreamSelector.STREAM, "orders")
                .select("orderId,userId,itemId,price");
        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        for (List<Object> rec : collector.actualResults) {
            for (Object field : rec) {
                System.out.print(field + ",");
            }
            System.out.println("");
        }
        Assert.assertEquals( orderStream.size(), collector.actualResults.size() );
    }

    @Test
    public void testInnerJoin() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users);
        ArrayList<Tuple> orderStream = makeStream("orders", orderFields, orders);
        TupleWindow window = makeTupleWindow(orderStream, userStream);

        WindowedQueryBolt bolt = new WindowedQueryBolt(WindowedQueryBolt.StreamSelector.STREAM, "users")
                .join("orders", "users", "userId")
                .select("userId,name,price");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        for (List<Object> rec : collector.actualResults) {
            for (Object field : rec) {
                System.out.print(field + ",");
            }
            System.out.println("");
        }
        Assert.assertEquals( orders.length, collector.actualResults.size() );
    }

    @Test
    public void testLeftJoin() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users);
        ArrayList<Tuple> orderStream = makeStream("orders", orderFields, orders);
        TupleWindow window = makeTupleWindow(orderStream, userStream);

        WindowedQueryBolt bolt = new WindowedQueryBolt(WindowedQueryBolt.StreamSelector.STREAM, "users")
                .leftJoin("orders", "users", "userId")
                .select("userId,name,price");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        for (List<Object> rec : collector.actualResults) {
            for (Object field : rec) {
                System.out.print(field + ",");
            }
            System.out.println("");
        }
        Assert.assertEquals(11, collector.actualResults.size() );
    }


    private TupleWindow makeTupleWindow(ArrayList<Tuple> stream) {
        return new TupleWindowImpl(stream, null, null);
    }


    private TupleWindow makeTupleWindow(ArrayList<Tuple> stream1, ArrayList<Tuple> stream2) {
        ArrayList<Tuple> combined = new ArrayList<>(stream1);
        combined.addAll(stream2);
        Collections.shuffle(combined);
        return new TupleWindowImpl(combined, null, null);
    }

    private ArrayList<Tuple> makeStream(String streamName, String[] fieldNames, Object[][] data) {

        ArrayList<Tuple> result = new ArrayList<>();
        MockContext mockContext = new MockContext(fieldNames);

        for (Object[] record : data) {
            TupleImpl rec = new TupleImpl(mockContext, Arrays.asList(record), 0, streamName);
            result.add( rec );
        }

        return result;
    }

    static class MockCollector extends OutputCollector {
        public ArrayList<List<Object> > actualResults = new ArrayList<>();

        public MockCollector() {
            super(null);
        }

        @Override
        public List<Integer> emit(List<Object> tuple) {
            actualResults.add(tuple);
            return null;
        }

    } // class MockCollector

    static class MockContext extends GeneralTopologyContext {

//        private final String[] fieldNames;
        private final Fields fields;

        public MockContext(String[] fieldNames) {
            super(null, null, null, null, null, null);
//            this.fieldNames = fieldNames;
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
