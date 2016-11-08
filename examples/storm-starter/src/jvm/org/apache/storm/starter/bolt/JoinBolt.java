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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JoinBolt extends BaseWindowedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(JoinBolt.class);
    private OutputCollector collector;
    int count=0;

    ArrayList<String> streamJoinOrder = new ArrayList<>(); // order in which to join the streams

    // Map[StreamName -> Map[Key -> List<Tuple>]  ]
    HashMap<String, HashMap<Object, ArrayList<TupleImpl> > > hashedInputs = new HashMap<>(); // holds remaining streams


    // Map[StreamName -> JoinInfo]
    HashMap<String, JoinInfo> joinCriteria = new HashMap<>();

    // Use streamId, source component name OR field in tuple to distinguish incoming tuple streams
    public enum  StreamSelector { STREAM, SOURCE_COMPONENT, FIELD }
    private final StreamSelector streamSelectorType;

    /**
     * StreamId to start the join with. Equivalent SQL ...
     *       select .... from streamId ...
     */
    public JoinBolt(StreamSelector type, String streamId) {
        // todo: support other types of stream selectors
        if( type!=StreamSelector.STREAM )
            throw new IllegalArgumentException(type.name());
        streamSelectorType = type;
        streamJoinOrder.add(streamId);
    }

    /**
     * Performs inner Join.  Equivalent SQL ..
     *     inner join streamId1 on streamId1.key1=streamId2.key2
     *
     *  Note: streamId2 must be previously joined
     *  Valid ex:    new JoinBolt(s1). join(s2,k2, s1,k1). join(s3,k3, s2,k2);
     *  Invalid ex:  new JoinBolt(s1). join(s3,k3, s2,k2). join(s2,k2, s1,k1);
     */
    public JoinBolt join(String streamId1, String key1, String streamId2, String key2) {
        hashedInputs.put(streamId1, new HashMap<Object, ArrayList<TupleImpl>>());
        joinCriteria.put(streamId1, new JoinInfo(key1, streamId2, key2, JoinType.INNER));
        streamJoinOrder.add(streamId1);
        return this;
    }

    /**
     * Performs inner Join on same key name in both streams.  Equivalent SQL ..
     *     inner join streamId1 on streamId1.key=streamId2.key
     */
    public JoinBolt join(String streamId1, String streamId2, String key) {
        return join(streamId1, key, streamId2, key);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("joined"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        //1) Reset hashedInputs
        for (int i = 1; i < streamJoinOrder.size(); i++) {
            hashedInputs.put(streamJoinOrder.get(i),  new HashMap<Object, ArrayList<TupleImpl>>());
        }

        // 2) Perform Join
        List<Tuple> currentWindow = inputWindow.get();
        JoinAccumulator joinResult = symmetricJoin(currentWindow);
        result = doProjection(joinResult);
        // 3) Emit results
//        collector.emit(new Values());
    }


    private JoinAccumulator symmetricJoin(List<Tuple> tuples) {
        JoinAccumulator probe = new JoinAccumulator();

        // 1) Build phase - first stream's tuples go into probeInputs, rest into HashMaps in hashedInputs
        String firstStream = streamJoinOrder.get(0);
        for (Tuple t : tuples) {
            TupleImpl tuple = (TupleImpl) t;
            String streamId = getStreamSelector(tuple);
            if( firstStream.equals(firstStream) ) {
                Object key = getKey(streamId, tuple);
                ArrayList<TupleImpl> recs = hashedInputs.get(streamId).get(key);
                if(recs == null) {
                    recs = new ArrayList<TupleImpl>();
                }
                recs.add(tuple);
                hashedInputs.get(streamId).put(key, recs);
            }  else {
                probe.insert(tuple);  // first stream's data goes into the probe
            }
        }

        // 2) Join the streams
        for (int i = 1; i < streamJoinOrder.size(); i++) {
            String streamName = streamJoinOrder.get(i) ;
            probe = doJoin(probe, hashedInputs.get(streamName), joinCriteria.get(streamName) );
        }

        return probe;
    }

    // Dispatches to the right join method based on the joinInfo.joinType
    private JoinAccumulator doJoin(JoinAccumulator probe, HashMap<Object, ArrayList<TupleImpl>> buildInput, JoinInfo joinInfo) {
        final JoinType joinType = joinInfo.getJoinType();
        switch ( joinType ) {
            case INNER:
                return doInnerJoin(probe, buildInput, joinInfo);
            case LEFT:
            case RIGHT:
            case OUTER:
            default:
                throw new RuntimeException("Unsupported join type : " + joinType.name() );
        }
    }

    private JoinAccumulator doInnerJoin(JoinAccumulator probe, Map<Object, ArrayList<TupleImpl> > buildInput, JoinInfo joinInfo) {
        String probeKeyName = joinInfo.getOtherKeyName();
        JoinAccumulator result = new JoinAccumulator();
        for (ResultRecord rec : probe.getRecords()) {
            Object probeKey = rec.getField(joinInfo.otherStream, probeKeyName);
            if(probeKey!=null) {
                ArrayList<TupleImpl> successfulJoin = buildInput.get(probeKey);
                result.insert( new ResultRecord(rec, successfulJoin) );
            }
        }
        return result;
    }


    // identify the key for the stream, and look it up in 'tuple'
    private Object getKey(String streamId, TupleImpl tuple) {
        String keyName = joinCriteria.get(streamId).getKeyName();
        return tuple.getValueByField(keyName);
    }


    private String getStreamSelector(TupleImpl ti) {
        switch (streamSelectorType) {
            case STREAM:
                return ti.getSourceStreamId();
            case SOURCE_COMPONENT:
            case FIELD:
            default:
                throw new RuntimeException(streamSelectorType + " stream selector type not yet supported");
        }
    }

    public static void main(String[] args) {
        new JoinBolt(StreamSelector.STREAM, "s0" )
                      .join("s1", "k1", "s2", "k2");
//                      .join("s3");
    }


    private enum JoinType {INNER, LEFT, RIGHT, OUTER}

    /** Describes how to join the other stream with the current stream */
    private static class JoinInfo implements Serializable {
        final static long serialVersionUID = 1L;

        String keyName;          // key of the current stream
        String otherStream;  // name of the other stream to join with
        String otherKeyName;     // key name from the other stream
        JoinType joinType;   // nature of join

        public JoinInfo(String key, String otherStream, String otherKey, JoinType joinType) {
            this.keyName = key;
            this.otherStream = otherStream;
            this.otherKeyName = otherKey;
            this.joinType = joinType;
        }

        public String getKeyName() {
            return keyName;
        }

        public String getOtherStream() {
            return otherStream;
        }

        public String getOtherKeyName() {
            return otherKeyName;
        }

        public JoinType getJoinType() {
            return joinType;
        }

    } // class JoinInfo

    // Join helper to concat fields to the record
    private static class ResultRecord {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<TupleImpl> tupleList = new ArrayList<>(); // one TupleImpl per Stream being joined

        public ResultRecord(TupleImpl tuple) {
            tupleList.add(tuple);
        }

        public ResultRecord(ResultRecord lhs, ArrayList<TupleImpl> rhs) {
            tupleList.addAll(lhs.tupleList);
            tupleList.addAll(rhs);
        }


        public void concat(TupleImpl tuple) {
            tupleList.add(tuple);
        }

        public Object getField(String stream, String fieldName) {
            for (TupleImpl tuple : tupleList) {
                if(tuple.getSourceStreamId().equals(stream))
                    return tuple.getValueByField(fieldName);
            }
            return null;
        }
    }

    private static class JoinAccumulator {
        ArrayList<ResultRecord> records = new ArrayList<>();

//        public JoinAccumulator(ArrayList<TupleImpl> initialTuples) {
//            for (TupleImpl tuple : initialTuples) {
//                records.add(new ResultRecord(tuple));
//            }
//        }
        public void insert(TupleImpl tuple) {
            records.add( new ResultRecord(tuple) );
        }

        public void insert(ResultRecord tuple) {
            records.add( tuple );
        }

        public Collection<ResultRecord> getRecords() {
            return records;
        }
    }
}
