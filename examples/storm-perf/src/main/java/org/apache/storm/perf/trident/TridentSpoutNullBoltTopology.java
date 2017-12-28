/*
/*
 * Copyright 2016 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.perf.trident;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 *
 */
public class TridentSpoutNullBoltTopology {
    public static final Logger LOG = LoggerFactory.getLogger(TridentSpoutNullBoltTopology.class);

    public static StormTopology getTopology(int batchsize, int parallelism) {
        TridentTopology topology = new TridentTopology();
        String fieldName = "f-1";
        topology.newStream("spout1", new TridentConstSpout(fieldName, "foo-" + new Date(), batchsize))
            .parallelismHint(parallelism)
            .each(new Fields(fieldName), new BaseFilter() {
                @Override
                public boolean isKeep(TridentTuple tuple) {
//                        String field = tuple.getStringByField(fieldName);
//                        LOG.info("############## field = [{}]", field);
                    return true;
                }
            }).parallelismHint(parallelism);

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        System.err.println(args.toString());

        int runTime = 60000;
        Config topoConf = new Config();
        int parallelism = 8;

        int batchSize = 8 * 1024;
        if (args.length > 6) {
            System.err.println("args: [parallelism=8] [batchSize=8k] [runDurationSec=60000] [optionalConfFile]");
            return;
        }

        if (args.length > 0) {
            parallelism = Integer.parseInt(args[0]);
            System.err.println("Parallelism = " + parallelism);
        }
        if (args.length > 1) {
            batchSize = Integer.parseInt(args[1]);
            System.err.println("batch = " + batchSize);
        }
        if (args.length > 2) {
            runTime = Integer.parseInt(args[2]);
            System.err.println("runTime = " + runTime);
        }
        if (args.length > 3) {
            topoConf.putAll(Utils.findAndReadConfigFile(args[4]));
            System.err.println("confFile = " + args[4]);
        }

        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(runTime, "trident-const-value-spout", topoConf, getTopology(batchSize, parallelism));
    }

}
