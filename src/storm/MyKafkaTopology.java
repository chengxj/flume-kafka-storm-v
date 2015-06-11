package storm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;

import entity.BillCharging;
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
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyKafkaTopology {

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
		public static Map<String, String> jsonToMap(String jsonStr) { 
			Map<String, String> ObjectMap = null; 
			Gson gson = new Gson(); 
			java.lang.reflect.Type type = new com.google.gson.reflect.TypeToken<Map<?,?>>() {}.getType(); 
			ObjectMap = gson.fromJson(jsonStr, type); 
			return ObjectMap; 
		}
		@Override
		public void execute(Tuple input) {
			String line = input.getString(0);
			LOG.info("RECV[kafka -> splitter] " + line);
			Map<String, String> jsonToMap = KafkaWordSplitter.jsonToMap(line);
			String api_k =  String.valueOf(jsonToMap.get("api_k"));
			System.out.println(api_k);
			if(api_k.equals("512.0") || api_k.equals("513.0")){
				collector.emit("processStream",input,new Values(jsonToMap));
				collector.ack(input);
			}
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("processStream",new Fields("sentence"));
		}
	}
	
	
	public static class ProcessBolt extends BaseRichBolt{
		private static final Log log = LogFactory.getLog(ProcessBolt.class);
		private static final long serialVersionUID = 7918290019347787140L;
		private OutputCollector collector;
		private Map<String, AtomicInteger> counterMap;
		@Override
		public void execute(Tuple input) {
			List<Object> list = input.getValues();
			Map<String,Map> billMap = new HashMap<String,Map>();
			for(Object obj :list){
				//ȡ����һ��tuple������
				Map map = (Map)obj;
				//ȡ����һ��tuple��id
				String id = (String) map.get("id");
				System.out.println("id==============================================="+id);
				
				if(billMap != null){
					//�ж�keymap���Ƿ���ڴ�������map
					if(billMap.containsKey(id)){
						Map valMap = billMap.get(id);
						valMap.put("chargetime", map.get("chargetime"));
						valMap.put("endtime", map.get("endtime"));
						//���ҵ�512��513֮����ύ����һ��bolt
						collector.emit(input,new Values(valMap));
						System.out.println("512+513============================================================"+valMap);
						collector.ack(input);
					}else{
						billMap.put(id, map);
					}
				}
			}
		}
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word-counter"));
		}
	}

	
	
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
		/**
		 * ��Ϣ�����巽��
		 */
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//Ĭ��ID����Ϣ������
			declarer.declare(new Fields("word", "count"));
		}
	}

	
	
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {
		String zks = "JSNJ-IVR-SRV-I620G10-22:2181,JSNJ-IVR-SRV-I620G10-23:2181,JSNJ-IVR-SRV-I620G10-24:2181";
		String topic = "kafkaToptic";
		String zkRoot = "/myKakfa"; // default zookeeper root configuration for
		String id = "word";
		BrokerHosts brokerHosts = new ZkHosts(zks,"/kafka/brokers");
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = false;
		spoutConf.zkServers = Arrays.asList(new String[] {
				"JSNJ-IVR-SRV-I620G10-22", "JSNJ-IVR-SRV-I620G10-23",
				"JSNJ-IVR-SRV-I620G10-24" });
		spoutConf.zkPort = 2181;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 2); 
		builder.setBolt("word-splitter", new KafkaWordSplitter(), 2).shuffleGrouping("kafka-reader");
		/*
		 * shuffleGrouping
		 * ��һ������ָ����һ��bolt��id
		 * �ڶ�������ָ����һ��bolt������ģ�declareStream��streamid
		 */
		builder.setBolt("word-counter", new ProcessBolt(),2).shuffleGrouping("word-splitter", "processStream");
		Config conf = new Config();
		String name = MyKafkaTopology.class.getSimpleName();
		if (args != null && args.length > 0) {
			conf.put(Config.NIMBUS_HOST, args[0]);
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(name, conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			Thread.sleep(60000);
			cluster.shutdown();
		}
	}
	
	
	
}
