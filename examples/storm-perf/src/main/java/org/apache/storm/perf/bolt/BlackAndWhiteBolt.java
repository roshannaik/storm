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

package org.apache.storm.perf.bolt;

import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.openimaj.image.MBFImage;

public class BlackAndWhiteBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Float[] result;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        result = new Float[] {0f, 0f, 0f};
    }

    @Override
    public void execute(Tuple tuple) {
        MBFImage bwFrame = ((MBFImage) tuple.getValue(0)).clone();
        for (int x = 0; x < bwFrame.getBounds().width; x++) {
            for (int y = 0; y < bwFrame.getBounds().height; y++) {
                Float[] p = bwFrame.getPixel(x, y);
                float avg = (p[0] * 0.21f + p[1] * 0.72f + p[2] * 0.07f);
                result[0] = avg;
                result[1] = avg;
                result[2] = avg;
                bwFrame.setPixel(x, y, result);
            }
        }

        ArrayList<Object> newTuple = new ArrayList<>(1);
        newTuple.add(bwFrame);
        collector.emit(newTuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("frame"));
    }

}
