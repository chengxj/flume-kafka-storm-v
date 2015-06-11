package storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class KuangGongTopology {
	
	private BrokerHosts brokerHosts = null;
	private String topic = null;
	
	public KuangGongTopology(String zkHosts, String brokerPath, String topic) {
		this.brokerHosts = new ZkHosts(zkHosts, brokerPath);
		this.topic = topic;
	}
	
	private StormTopology builTopology() {
		SpoutConfig spoutConf = new SpoutConfig(this.brokerHosts, this.topic, "/offsets", "kuanggong");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("logs", new KafkaSpout(spoutConf), 8);
		builder.setBolt("word-splitter", new KafkaWordSplitter(), 2).globalGrouping("logs");
		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping(
				"word-splitter", new Fields("word"));
		
		return builder.createTopology();
	}
	
	public static void main(String[] args) {
		String zkHosts = "JSNJ-IVR-SRV-I620G10-22:2181,JSNJ-IVR-SRV-I620G10-23:2181,JSNJ-IVR-SRV-I620G10-24:2181";
		String brokerPath = "/kafka/brokers";
		String topic = "kafkaToptic";
		
		KuangGongTopology kuangGongTopology = new KuangGongTopology(zkHosts, brokerPath, topic);
		StormTopology stormTopology = kuangGongTopology.builTopology();
		
		Config config = new Config();
		
		if (args != null && args.length > 0) {
			String name = args[0];
			config.setNumWorkers(4);
			try {
				StormSubmitter.submitTopology(name, config, stormTopology);
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			List<String> zkServerList = new ArrayList<String>();
			zkServerList.add("JSNJ-IVR-SRV-I620G10-22");
			zkServerList.add("JSNJ-IVR-SRV-I620G10-23");
			zkServerList.add("JSNJ-IVR-SRV-I620G10-24");
			config.put(Config.STORM_ZOOKEEPER_SERVERS, zkServerList);
			config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
			config.setDebug(true);
			
			config.setNumWorkers(4);
			config.setNumAckers(1);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("kuanggong", config, stormTopology);
		}
	}
	/**
	 * 单词计数
	 * @author simon
	 *
	 */
	public static class WordCounter extends BaseRichBolt {

		private static final Log LOG = LogFactory.getLog(WordCounter.class);
		private static final long serialVersionUID = 886149197481637894L;
		private OutputCollector collector;
		private Map<String, AtomicInteger> counterMap;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
			this.counterMap = new HashMap<String, AtomicInteger>();
		}

		@Override
		public void execute(Tuple input) {
			String word = input.getString(0);
			int count = input.getInteger(1);
			LOG.info("RECV[splitter -> counter] " + word + " : " + count);
			AtomicInteger ai = this.counterMap.get(word);
			if (ai == null) {
				ai = new AtomicInteger();
				this.counterMap.put(word, ai);
			}
			ai.addAndGet(count);
			collector.ack(input);
			LOG.info("CHECK statistics map: " + this.counterMap);
		}

		@Override
		public void cleanup() {
			LOG.info("The final result:");
			Iterator<Entry<String, AtomicInteger>> iter = this.counterMap
					.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, AtomicInteger> entry = iter.next();
				LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}
	
	/**
	 * 分词
	 * @author simon
	 *
	 */
	public static class KafkaWordSplitter extends BaseRichBolt {

		private static final Log LOG = LogFactory
				.getLog(KafkaWordSplitter.class);
		private static final long serialVersionUID = 886149197481637894L;
		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input) {
			String line = input.getString(0);
			System.out.println(line);
			LOG.info("RECV[kafka -> splitter] " + line);
			String[] words = line.split("\\s+");
			for (String word : words) {
				LOG.info("EMIT[splitter -> counter] " + word);
				collector.emit(input, new Values(word, 1));
			}
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}

	}

}
