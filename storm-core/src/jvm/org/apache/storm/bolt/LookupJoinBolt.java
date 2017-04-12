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

import com.google.common.collect.LinkedListMultimap;
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
    private String dataStream;
    private String lookupStream;

    protected FieldSelector[] outputFields = null;  // specified via bolt.select() ... used in declaring Output fields
    private String outputStream;
    private int retentionTime;
    private int retentionCount;
    private boolean timeBasedRetention;

    private LinkedListMultimap<String, Tuple> lookupBuffer;

    private ArrayDeque<Long> timeTracker; // for time based retention
    private ArrayList<JoinInfo> joinCriteria = new ArrayList<>();

    private OutputCollector collector;


    protected enum JoinType {INNER, LEFT}
    private JoinType joinType;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        String[] outputFieldNames = new String[outputFields.length];
        for( int i=0; i<outputFields.length; ++i ) {
            outputFieldNames[i] = outputFields[i].getOutputName() ;
        }
        if (outputStream !=null) {
            declarer.declareStream(outputStream, new Fields(outputFieldNames));
        } else {
            declarer.declare(new Fields(outputFieldNames));
        }
    }



    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        if (timeBasedRetention) {
            lookupBuffer =  LinkedListMultimap.create(50_000);
            timeTracker = new ArrayDeque<Long>();
        } else { // count Based Retention
            lookupBuffer = LinkedListMultimap.create(retentionCount);
        }
    }

//    @Override
//    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//        this.collector = collector;
//        if (timeBasedRetention) { //  expiration handled explicitly
//            lookupBuffer =  LinkedListMultimap.create(50_000);
//            timeTracker = new ArrayDeque<Long>();
//        } else { // count Based Retention
//            lookupBuffer = LinkedListMultimap.create(retentionCount) {
//                @Override
//                protected boolean removeEldestEntry(Map.Entry<Object, Tuple> eldest) {
//                    boolean shouldRetire = size() > retentionCount;
//                    if (shouldRetire) {
//                        collector.ack(eldest.getValue());
//                    }
//                    return shouldRetire;
//                }
//            };
//        }
//    }

    // Use streamId, source component name OR field in tuple to distinguish incoming tuple streams
    public enum Selector { STREAM, SOURCE }
    protected final Selector selectorType;

    /**
     * Calls  LookupJoinBolt(Selector.SOURCE, dataStreamSelector)
     * @param streamType   Specifies whether 'dataStream' refers to a stream name or source component id.
     */
    public LookupJoinBolt(Selector streamType) {
        selectorType = streamType;
    }


    /**
     * Field to use for join. Can be nested field name x.y.z  (assumes x & y are of type Map<> )
     * @param dataStreamField    Field to use for join on data stream. Can be nested field name x.y.z  (assumes x & y are of type Map<> )
     * @param lookupStreamField  Field to use for join on lookup stream.
     * @return
     */
    public LookupJoinBolt joinOn(String dataStreamField, String lookupStreamField) {
        FieldSelector dataField = new FieldSelector(dataStream, dataStreamField);
        FieldSelector lookupField = new FieldSelector(lookupStream, lookupStreamField);

        if( dataField.equals(lookupField) ) {
            throw new IllegalArgumentException("Both field selectors refer to same field: " + dataField.getOutputName());
        }
        joinCriteria.add(new JoinInfo(lookupField, dataField));
        return this;
    }


    /**
     * Introduces the buffered 'LookupStream' and its retention policy
     * @param lookupStream   Name of the stream (or source component Id) to be treated as a buffered 'LookupStream'
     * @param retentionCount How many records to retain.
     * @return
     */
    public LookupJoinBolt lookupStream(String lookupStream, BaseWindowedBolt.Count retentionCount) {
        if(this.lookupStream!=null) {
            throw  new IllegalArgumentException("Cannot declare a Lookup stream more than once");
        }
        this.lookupStream = lookupStream;
        this.retentionCount = retentionCount.value;
        this.joinType = JoinType.INNER;
        this.timeBasedRetention = false;
        return this;
    }

    public LookupJoinBolt lookupStream(String lookupStream, BaseWindowedBolt.Duration retentionTime) {
        if(this.lookupStream!=null) {
            throw  new IllegalArgumentException("Cannot declare a Lookup stream more than once");
        }
        this.lookupStream = lookupStream;
        this.joinType = JoinType.INNER;
        this.retentionTime = retentionTime.value;
        this.timeBasedRetention = true;
        return this;
    }


    public LookupJoinBolt dataStream(String dataStream) {
        if(this.dataStream!=null) {
            throw  new IllegalArgumentException("Cannot declare a Data stream more than once");
        }
        this.dataStream = dataStream;
        this.joinType = JoinType.INNER;
        return this;
    }


//    public LookupJoinBolt dataStream(String dataStream, BaseWindowedBolt.Count retentionCount) {
//        if(this.dataStream!=null) {
//            throw  new IllegalArgumentException("Cannot declare a Data stream more than once");
//        }
//        this.dataStream = dataStream;
//        this.retentionCount = retentionCount.value;
//        timeBasedRetention = false;
//        this.joinType = JoinType.INNER;
//        return this;
//    }
//
    /**
     * Specify output fields
     *      e.g: .select("lookupField, stream2:dataField, field3")
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
        this.outputStream = streamName;
        return this;
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = getStreamSelector(tuple);
        if (timeBasedRetention) {
            expireAndAckTimedOutEntries(lookupBuffer);
        }
        if ( isLookupStream(streamId) ) {
            String key = makeLookupTupleKey(tuple);
            lookupBuffer.put(key, tuple);
            if(timeBasedRetention) {
                timeTracker.add(System.currentTimeMillis());
            } else {  // count based Rotation
                if (lookupBuffer.size() > retentionCount) {
                    Tuple expired = removeHead(lookupBuffer);
                    collector.ack(expired);
                }
            }
        } else if (isDataStream(streamId) ) {
            List<Tuple> matches = joinWithLookupStream(tuple);
            if (matches==null && joinType==JoinType.LEFT) {
                ArrayList<Object> outputTuple = doProjection(tuple, null);
                emit(outputTuple, tuple);
                collector.ack(tuple);
                return;
            }

            for (Tuple lookupTuple : matches) {
                ArrayList<Object> outputTuple = doProjection(lookupTuple, tuple);
                emit(outputTuple, tuple, lookupTuple);
            }
            collector.ack(tuple);
        } else {
            LOG.warn("Received tuple from unexpected stream/source : {}. Tuple will be dropped.", streamId);
        }
    }

    private String makeLookupTupleKey(Tuple tuple) {
        StringBuffer key = new StringBuffer();
        for (JoinInfo ji : joinCriteria) {
            String partialKey = findField(ji.lookupField, tuple).toString();
            key.append( partialKey );
            key.append(".");
        }
        return key.toString();
    }

    private String makeDataTupleKey(Tuple tuple) {
        StringBuffer key = new StringBuffer();
        for (JoinInfo ji : joinCriteria) {
            String partialKey = findField(ji.dataField, tuple).toString();
            key.append( partialKey );
            key.append(".");
        }
        return key.toString();
    }

    // returns null if no match
    private List<Tuple> joinWithLookupStream(Tuple lookupTuple) {
        String key = makeDataTupleKey(lookupTuple);
        return lookupBuffer.get(key);
    }

    // Removes timedout entries from lookupBuffer & timeTracker. Acks tuples being expired.
    private void expireAndAckTimedOutEntries(LinkedListMultimap<String, Tuple> lookupBuffer) {
        Long expirationTime = System.currentTimeMillis() - retentionTime;
        Long  insertionTime = timeTracker.peek();
        while ( insertionTime!=null && expirationTime.compareTo(insertionTime) > 0) {
            Tuple expired = removeHead(lookupBuffer);
            timeTracker.pop();
            collector.ack(expired);
            insertionTime = timeTracker.peek();
        }
    }

    private static Tuple removeHead(LinkedListMultimap<String, Tuple> lookupBuffer) {
        List<Map.Entry<String, Tuple>> entries = lookupBuffer.entries();
        return entries.remove(0).getValue();
    }


    private void emit(ArrayList<Object> outputTuple, Tuple dataTuple) {
        Tuple anchor = dataTuple;
        if ( outputStream ==null )
            collector.emit(anchor, outputTuple);
        else
            collector.emit(outputStream, anchor, outputTuple);
    }

    private void emit(ArrayList<Object> outputTuple, Tuple dataTuple, Tuple lookupTuple) {
        List<Tuple> anchors = Arrays.asList(dataTuple, lookupTuple);
        if ( outputStream ==null )
            collector.emit(anchors, outputTuple);
        else
            collector.emit(outputStream, anchors, outputTuple);
    }

    private boolean isDataStream(String streamId) {
        return streamId.equals(dataStream);
    }

    private boolean isLookupStream(String streamId) {
        return streamId.equals(lookupStream);
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
        if (tuple==null) {
            return null;
        }
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

    /** Performs projection on the tuples based on 'projectionFields'
     *
     * @param tuple1   can be null
     * @param tuple2   can be null
     * @return   project fields
     */

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

}


class FieldSelector implements Serializable {
    final static long serialVersionUID = 2L;

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
        this(stream + ":" + fieldDescriptor);
        if(fieldDescriptor.indexOf(":")>=0) {
            throw new IllegalArgumentException("Not expecting stream qualifier ':' in '" + fieldDescriptor
                    + "'. Stream name '" + stream +  "' is implicit in this context");
        }
        this.streamName = stream;
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

    public String getFullName() {
        if(streamName!=null)
            return streamName + ":" + field;
        return getOutputName();
    }

    @Override
    public String toString() {
        return outputName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        try {
            FieldSelector that = (FieldSelector) o;
            return outputName != null ? outputName.equals(that.outputName) : that.outputName == null;
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return outputName != null ? outputName.hashCode() : 0;
    }
}


class JoinInfo implements Serializable {
    final static long serialVersionUID = 1L;

    FieldSelector lookupField;           // field for the current stream
    FieldSelector dataField;      // field for the other (2nd) stream


    public JoinInfo(FieldSelector lookupField, FieldSelector dataField) {
        this.lookupField = lookupField;
        this.dataField = dataField;
    }

    public FieldSelector getLookupField() {
        return lookupField;
    }

    public String[] getDataField() {
        return dataField.getField();
    }

} // class JoinInfo