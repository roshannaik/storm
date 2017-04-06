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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


public class LookupJoinBolt extends BaseRichBolt  {
    private static final Logger LOG = LoggerFactory.getLogger(LookupJoinBolt.class);

    protected FieldSelector[] outputFields = null;  // specified via bolt.select() ... used in declaring Output fields
    private String outputStreamName;
    private final FieldSelector dataStreamSelector;
    private FieldSelector lookupStreamSelector = null;
    private int retentionTime;
    private int retentionCount;
    private boolean timeBasedRetention;

    LinkedHashMap<Object, Tuple> lookupBuffer;
    private ArrayDeque<Long> timeTracker; // for time based retention

    private OutputCollector collector;


    protected enum JoinType {INNER, LEFT}
    private JoinType joinType;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        String[] outputFieldNames = new String[outputFields.length];
        for( int i=0; i<outputFields.length; ++i ) {
            outputFieldNames[i] = outputFields[i].getOutputName() ;
        }
        if (outputStreamName!=null) {
            declarer.declareStream(outputStreamName, new Fields(outputFieldNames));
        } else {
            declarer.declare(new Fields(outputFieldNames));
        }
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        if (timeBasedRetention) { //  expiration handled explicitly
            lookupBuffer = new LinkedHashMap<Object, Tuple>();
            timeTracker = new ArrayDeque<Long>();
        } else { // count Based Retention
            lookupBuffer = new LinkedHashMap<Object, Tuple>(retentionCount) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Object, Tuple> eldest) {
                    boolean shouldRetire = size() > retentionCount;
                    if (shouldRetire) {
                        collector.ack(eldest.getValue());
                    }
                    return shouldRetire;
                }
            };
        }
    }

    // Use streamId, source component name OR field in tuple to distinguish incoming tuple streams
    public enum Selector { STREAM, SOURCE }
    protected final Selector selectorType;


    /**
     * Calls  LookupJoinBolt(Selector.SOURCE, dataStreamSelector, fieldName)
     * @param dataStream      Refers to the source component id (spout/bolt) whose data is to be treated as the realtime "DataStream".
     * @param joinField       Field to use for join. can be nested field name x.y.z  (assumes x & y are of type Map<> )
     */
    public LookupJoinBolt(String dataStream, String joinField) {
        this(Selector.SOURCE, dataStream, joinField);
    }

    /**
     * Calls  LookupJoinBolt(Selector.SOURCE, dataStreamSelector)
     * @param streamType   Specifies whether 'dataStream' refers to a stream name or source component id.
     * @param dataStream   Refers to the source component (spout/bolt) whose data is to be treated as the realtime "DataStream".
     * @param joinField    Field to use for join. Can be nested field name x.y.z  (assumes x & y are of type Map<> )
     */
    public LookupJoinBolt(Selector streamType, String dataStream, String joinField) {
        selectorType = streamType;
        this.dataStreamSelector = new FieldSelector(dataStream, joinField);
    }


    /**
     * Introduces the buffered 'LookupStream' and the field to use for INNER join
     * @param lookupStream   Name of the stream (or source component Id) to be treated as a buffered 'LookupStream'
     * @param joinField      Field to join with. Can be nested field name x.y.z  (assumes x & y are of type Map<> )
     * @param retentionCount How many records to retain.
     * @return
     */
    public LookupJoinBolt join(String lookupStream, String... joinField, BaseWindowedBolt.Count retentionCount) {
        if(lookupStream==null) {
            throw  new IllegalArgumentException("Only two streams can  be joined at a time. Cannot call join/leftJoin() more than once");
        }
        this.retentionCount = retentionCount.value;
        this.lookupStreamSelector = new FieldSelector(lookupStream, joinField);
        this.joinType = JoinType.INNER;
        this.timeBasedRetention = false;
        return this;
    }

    public LookupJoinBolt join(String lookupStream, String joinField,  BaseWindowedBolt.Duration retentionTime) {
        if(lookupStreamSelector!=null) {
            throw  new IllegalArgumentException("Only two streams can  be joined at a time. Cannot call join/leftJoin() more than once");
        }
        this.lookupStreamSelector = new FieldSelector(lookupStream, joinField);
        this.joinType = JoinType.INNER;
        this.retentionTime = retentionTime.value;
        this.timeBasedRetention = true;
        return this;
    }


    /**
     * Specify output fields
     *      e.g: .select("field1, stream2:field2, field3")
     * Nested Key names are supported for nested types:
     *      e.g: .select("outerKey1.innerKey1, outerKey1.innerKey2, stream3:outerKey2.innerKey3)"
     * Inner types (non leaf) must be Map<> in order to support nested lookup using this dot notation
     * This selected fields implicitly declare the output fieldNames for the bolt based.
     * @param commaSeparatedKeys
     * @return
     */
    public LookupJoinBolt select(String commaSeparatedKeys) {
        String[] fieldNames = commaSeparatedKeys.split(",");

        outputFields = new FieldSelector[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            outputFields[i] = new FieldSelector(fieldNames[i]);
        }
        return this;
    }

    public LookupJoinBolt withOutputStream(String streamName) {
        this.outputStreamName = streamName;
        return this;
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = getStreamSelector(tuple);
        if (timeBasedRetention) {
            expireTimedOutEntries(lookupBuffer);
        }
        if ( isLookupStream(streamId) ) {
            Object key = findField(lookupStreamSelector, tuple);
            lookupBuffer.put(key, tuple);
            if(timeBasedRetention) {
                timeTracker.add(System.currentTimeMillis());
            }
        } else if (isDataStream(streamId) ) {
            Object key = findField(dataStreamSelector, tuple);
            Tuple lookupTuple = lookupBuffer.get(key);
            if (lookupTuple!=null) {
                ArrayList<Object> outputTuple = doProjection(tuple, lookupTuple);
                emit(outputTuple, tuple, lookupTuple);
            } else if (joinType==JoinType.LEFT) { // && lookupTuple==null
                ArrayList<Object> outputTuple = padNullFields(tuple);
                emit(outputTuple, tuple, lookupTuple);
            }
            collector.ack(tuple);
        } else {
            LOG.warn("Received tuple from unexpected stream/source : {}. Tuple will be dropped.", streamId);
        }
    }

    private void expireTimedOutEntries(LinkedHashMap<Object, Tuple> lookupBuffer) {
        Long expirationTime = System.currentTimeMillis() - retentionTime;
        Long  insertionTime = timeTracker.peek();
        while ( insertionTime!=null && expirationTime.compareTo(insertionTime) > 0) {
            removeOldest(lookupBuffer);
            timeTracker.pop();
            insertionTime = timeTracker.peek();
        }
    }

    private static void removeOldest(LinkedHashMap<Object, Tuple> lookupBuffer) {
        Iterator<Map.Entry<Object, Tuple>> itr = lookupBuffer.entrySet().iterator();
        itr.next();
        itr.remove();
    }

    private void emit(ArrayList<Object> outputTuple, Tuple dataTuple, Tuple lookupTuple) {
        List<Tuple> anchors = Arrays.asList(dataTuple, lookupTuple);
        collector.emit(anchors, outputTuple);
    }

    private boolean isDataStream(String streamId) {
        return streamId.equals(dataStreamSelector.getStreamName());
    }

    private boolean isLookupStream(String streamId) {
        return streamId.equals(lookupStreamSelector.getStreamName());
    }

    // Returns either the source component name or the stream name for the tuple
    private String getStreamSelector(Tuple ti) {
        switch (selectorType) {
            case STREAM:
                return ti.getSourceStreamId();
            case SOURCE:
                return ti.getSourceComponent();
            default:
                throw new RuntimeException(selectorType + " stream selector type not yet supported");
        }
    }

    // Extract the field from tuple. Field may be nested field (x.y.z)
    protected Object findField(FieldSelector fieldSelector, Tuple tuple) {
        // very stream name matches, it stream name was specified
        if ( fieldSelector.streamName!=null &&
                !fieldSelector.streamName.equalsIgnoreCase( getStreamSelector(tuple) ) ) {
            return null;
        }

        Object curr = null;
        for (int i=0; i < fieldSelector.field.length; i++) {
            if (i==0) {
                if (tuple.contains(fieldSelector.field[i]) )
                    curr = tuple.getValueByField(fieldSelector.field[i]);
                else
                    return null;
            }  else  {
                curr = ((Map) curr).get(fieldSelector.field[i]);
                if (curr==null)
                    return null;
            }
        }
        return curr;
    }

    // Performs projection on the tuples based on 'projectionFields'
    protected ArrayList<Object> doProjection(Tuple tuple1, Tuple tuple2) {
        ArrayList<Object> result = new ArrayList<>(outputFields.length);
        for ( int i = 0; i < outputFields.length; i++ ) {
            FieldSelector outField = outputFields[i];
            Object field = findField(outField, tuple1) ;
            if (field==null)
                field = findField(outField, tuple2);
            result.add(field); // adds null if field is not found in both tuples
        }
        return result;
    }

    protected ArrayList<Object> padNullFields(Tuple tuple1) {
        ArrayList<Object> result = new ArrayList<>(outputFields.length);
        for ( int i = 0; i < outputFields.length; i++ ) {
            FieldSelector outField = outputFields[i];
            Object field = findField(outField, tuple1) ;
            result.add(field); // adds null if field is not found in tuple1
        }
        return result;
    }


}

class FieldSelector implements Serializable {
    String streamName;    // can be null;
    String[] field;       // nested field "x.y.z"  becomes => String["x","y","z"]
    String outputName;    // either "stream1:x.y.z" or "x.y.z" depending on whether stream name is present.

    public FieldSelector(String fieldDescriptor)  {  // sample fieldDescriptor = "stream1:x.y.z"
        int pos = fieldDescriptor.indexOf(':');

        if (pos>0) {  // stream name is specified
            streamName = fieldDescriptor.substring(0,pos).trim();
            outputName = fieldDescriptor.trim();
            field =  fieldDescriptor.substring(pos+1, fieldDescriptor.length()).split("\\.");
            return;
        }

        // stream name unspecified
        streamName = null;
        if(pos==0) {
            outputName = fieldDescriptor.substring(1, fieldDescriptor.length() ).trim();

        } else if (pos<0) {
            outputName = fieldDescriptor.trim();
        }
        field =  outputName.split("\\.");
    }

    /**
     * @param stream name of stream
     * @param fieldDescriptor  Simple fieldDescriptor like "x.y.z" and w/o a 'stream1:' stream qualifier.
     */
    public FieldSelector(String stream, String fieldDescriptor)  {
        this(fieldDescriptor);
        if(fieldDescriptor.indexOf(":")>=0) {
            throw new IllegalArgumentException("Not expecting stream qualifier ':' in '" + fieldDescriptor
                    + "'. Stream name '" + stream +  "' is implicit in this context");
        }
        this.streamName = stream;
    }

    public FieldSelector(String stream, String[] field)  {
        this( stream, String.join(".", field) );
    }


    public String getStreamName() {
        return streamName;
    }

    public String[] getField() {
        return field;
    }

    public String getOutputName() {
        return toString();
    }

    @Override
    public String toString() {
        return outputName;
    }
}