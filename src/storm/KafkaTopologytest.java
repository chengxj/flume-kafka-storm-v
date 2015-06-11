package storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class KafkaTopologytest {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new KafkaSpouttest("myTopic"), 1);
		builder.setBolt("bolt1", new Bolt1(), 2).shuffleGrouping("spout");
		builder.setBolt("bolt2", new Bolt2(), 2).fieldsGrouping("bolt1",
				new Fields("word"));

		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 1);
		conf.put(Config.TOPOLOGY_DEBUG, true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("my-flume-kafka-storm-topology-integration",
				conf, builder.createTopology());

		Utils.sleep(1000*60*5); // local cluster test ...
		cluster.shutdown();
	}

	public static class Bolt1 extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				String msg = input.getString(0);
				int id = input.getInteger(1);
				String time = input.getString(2);
				msg = msg + " bolt1";
				System.out.println("对消息加工第1次-------[arg0]:" + msg
						+ "---[arg1]:" + id + "---[arg2]:" + time + "------->"
						+ msg);
				if (msg != null) {
					collector.emit(new Values(msg));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	public static class Bolt2 extends BaseBasicBolt {
		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String msg = tuple.getString(0);
			msg = msg + " bolt2";
			System.out.println("对消息加工第2次---------->" + msg);
			collector.emit(new Values(msg, 1));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}
}
