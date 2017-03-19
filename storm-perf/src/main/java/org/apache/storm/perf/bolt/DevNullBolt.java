package org.apache.storm.perf.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ThroughputMeter;

import java.util.Map;


public class DevNullBolt extends BaseRichBolt {
    private OutputCollector collector;
    ThroughputMeter meter;
    public static final  org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DevNullBolt.class);
    long count =0;
    int printFreq =0;

    public DevNullBolt() {
        super();
        printFreq= 10_000_000;
    }

    public DevNullBolt(int printFreq) {
        super();
        this.printFreq = printFreq;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);
        if(meter==null) {
            ++count;
            if(count==5_000_000) {
                this.meter = new ThroughputMeter("DevNull Bolt", printFreq);
            }
        } else {
            meter.record();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {
        super.cleanup();
        LOG.error(" =====> %,d k/s ", meter.stop()/1000);

    }
}
