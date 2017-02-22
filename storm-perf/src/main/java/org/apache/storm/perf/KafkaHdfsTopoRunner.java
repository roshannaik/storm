package org.apache.storm.perf;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.KillOptions;


public class KafkaHdfsTopoRunner {
  public static void main(String[] args) throws Exception {
    LocalCluster cluster = new LocalCluster();

//    cluster.submitTopology("topo1", getConfig(), KafkaHdfsTopo.getTopology("/Users/rnaik/Projects/idea/storm-golden/examples/storm-starter/src/jvm/org/apache/storm/starter/perf/conf/KafkaHdfs.yaml"));
//    cluster.submitTopology("topo1", getConfig(), KafkaSpoutNullBoltTopo.getTopology("/Users/rnaik/Projects/idea/storm-golden/examples/storm-starter/src/jvm/org/apache/storm/starter/perf/conf/KafkaHdfs.yaml"));
//    cluster.submitTopology("topo1", getConfig(), HdfsBoltTopo.getTopology("/Users/rnaik/Projects/idea/storm-golden/examples/storm-starter/src/jvm/org/apache/storm/starter/perf/conf/KafkaHdfs.yaml"));
    // 2 - Print metrics every 60 sec, killAndExit topology after 30 min
    for (int i = 0; i < 5; i++) {
      try {
        Thread.sleep(10 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
//      FastWordCountTopology.printMetrics(client, TOPOLOGY_NAME);
    }

    // 3- Kill
    KillOptions opts = new KillOptions();
    opts.set_wait_secs(0);
    cluster.killTopologyWithOpts("topo1", opts);

  }
  public static Config getConfig() {
    return new Config();
  }
}
