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

package org.apache.storm.perf.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.openimaj.image.MBFImage;
import org.openimaj.video.capture.Device;
import org.openimaj.video.capture.VideoCapture;
import org.openimaj.video.capture.VideoCaptureException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class CameraSpout extends BaseRichSpout {

    private static final String DEFAUT_FIELD_NAME = "frame";
    private String fieldName = DEFAUT_FIELD_NAME;
    private SpoutOutputCollector collector = null;
    private int count = 0;
    private Long sleep = 0L;
    private int ackCount = 0;
    private VideoCapture vid = null;
    private Iterator<MBFImage> frameItr = null;
    String[] outStreams;

    public CameraSpout(String... streams) {
        if (streams==null || streams.length==0) {
            streams = new String[]{Utils.DEFAULT_STREAM_ID};
        }
        outStreams = streams;
    }

    public CameraSpout withOutputFields(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (outStreams==null || outStreams.length==0) {
            declarer.declare(new Fields(fieldName));
        } else {
            for (String stream : outStreams) {
                declarer.declareStream(stream, new Fields(fieldName));
            }
        }
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (frameItr == null) {
            try {
                Device defaultDevice = VideoCapture.getVideoDevices().get(0);
                this.vid = new VideoCapture(320, 240); // , 60, defaultDevice
                this.frameItr = vid.iterator();
            } catch (VideoCaptureException e) {
                throw new RuntimeException(e);
            }
        }
        MBFImage frame = frameItr.next();
        if (outStreams==null || outStreams.length==0) {
            collector.emit(new Values(frame));
        } else {
            collector.emit(outStreams[0], new Values(frame));
            for (int i = 1; i < outStreams.length; i++) {
                collector.emit(outStreams[i], new Values(frame.clone()));
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        ++ackCount;
        super.ack(msgId);
    }
}
