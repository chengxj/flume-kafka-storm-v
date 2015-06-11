package storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.gson.Gson;

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
import storm.KuangGongTopology.KafkaWordSplitter;
import storm.KuangGongTopology.WordCounter;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class ChargingTopology {	
	
	private BrokerHosts brokerHosts = null;
	private String topic = null;
	
	public ChargingTopology(String zkHosts, String brokerPath, String topic) {
		this.brokerHosts = new ZkHosts(zkHosts, brokerPath);
		this.topic = topic;
	}
	
	/**
	 * 构建topology
	 * @return
	 * @author simon
	 * @date 2015年6月9日 下午4:14:25
	 */
	private StormTopology builTopology() {
		SpoutConfig spoutConf = new SpoutConfig(this.brokerHosts, this.topic, "/offsets", "charging");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		//设置线程数为8
		builder.setSpout("charging_logs", new KafkaSpout(spoutConf), 3);
		builder.setBolt("Sentence-Filter", new SentenceFilter(), 2).globalGrouping("charging_logs");
//		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping(
//				"word-splitter", new Fields("word"));
		return builder.createTopology();
	}
	
	public static void main(String[] args) {
		String zkHosts = "JSNJ-IVR-SRV-I620G10-22:2181,JSNJ-IVR-SRV-I620G10-23:2181,JSNJ-IVR-SRV-I620G10-24:2181";
		String brokerPath = "/kafka/brokers";
		String topic = "kafkaToptic";
		
		ChargingTopology chargingTopology = new ChargingTopology(zkHosts, brokerPath, topic);
		StormTopology stormTopology = chargingTopology.builTopology();
		
		Config config = new Config();
		if (args != null && args.length > 0) {
			//集群环境
			String name = args[0];
			//设置工作进程
			config.setNumWorkers(4);
			try {
				StormSubmitter.submitTopology(name, config, stormTopology);
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			//本地环境
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
			localCluster.submitTopology(ChargingTopology.class.getSimpleName(), config, stormTopology);
		}
	}
	/**
	 * 只取512,513的消息
	 * @author simon
	 *
	 */
	public static class SentenceFilter extends BaseRichBolt {

		private static final Log LOG = LogFactory
				.getLog(SentenceFilter.class);
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
			Map<String, String> ObjectMap = null; 
			Gson gson = new Gson(); 
			java.lang.reflect.Type type = new com.google.gson.reflect.TypeToken<Map<?,?>>() {}.getType(); 
			ObjectMap = gson.fromJson(line, type);
			
			LOG.info("RECV[kafka -> splitter] " + line);
//			Map id_map = new HashMap<String,Map<String, String>>();
//				LOG.info("EMIT[splitter -> counter] " + word);
//			collector.emit(input,list);
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}
	
}
