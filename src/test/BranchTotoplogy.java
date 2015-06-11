package test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class BranchTotoplogy {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MySpout());
		builder.setBolt("add", new MyAddBolt()).shuffleGrouping("spout");
		builder.setBolt("addten", new MyAddTenBolt()).shuffleGrouping("add","addtenstrim");
//		builder.setBolt("out", new Print()).shuffleGrouping("addten").shuffleGrouping("add","printstrim");
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCount", conf, builder.createTopology());
		Thread.sleep(60000);
		cluster.shutdown();
		}
}
