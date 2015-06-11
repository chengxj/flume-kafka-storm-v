package storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.RandomSentenceSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountTopology {
	public static class SplitSentence extends BaseBasicBolt {
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				String msg = input.getString(0);
				System.out.println(msg + "-------------------");
				if (msg != null) {
					String[] s = msg.split(" ");
					for (String string : s) {
						collector.emit(new Values(string));
					}
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

	public static class WordCount extends BaseBasicBolt {
		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String word = tuple.getString(0);
			Integer count = counts.get(word);
			if (count == null)
				count = 0;
			count++;
			counts.put(word, count);
			collector.emit(new Values(word, count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
		
		@Override
		public void cleanup() {
		    /**
		     * ע���ڿ���Boltʱ��,���ǵ�topology��Storm��Ⱥ������ʱ Bolt��cleanup()������һ��ִ��,
		     * ���ڼ�Ⱥ�в���ʹ��cleanup()���� 
		     * �����������ڿ���ģʽ��ʹ�ø÷���,���ܱ�֤�÷����ᱻִ��
		     */
		    System.out.println("------------------ count result ------------------");
		    List<String> list = new ArrayList<String>();
		    list.addAll(this.counts.keySet());
		    for (String sKey : list) {
		      System.out.println(sKey + " : " + this.counts.get(sKey));
		    }
		    System.out.println("---------------------------------------------------");
		  }
	}
	

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
//	 	��ʾ"spout"���߳���Ϊ5����һ���߳�������1������
		builder.setSpout("spout", new RandomSentenceSpout(), 5);
		
		builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping(
				"spout");
		builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split",
				new Fields("word"));
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if (args != null && args.length > 0) {
			/*
			 * ���ø�topology��storm��Ⱥ��Ҫ��ռ����Դslot����һ��slot��Ӧ��supervisor�ڵ��ϵ��Ը�worker����
			 * ���������spot���������������ڵ���ӵ�е�worker��Ŀ�Ļ����п����ύ���ɹ���������ļ�Ⱥ�����Ѿ�����
			 * һЩtopology�����ڻ�ʣ��2��worker��Դ
			 * ��������ڴ��������4�������topology�Ļ�����ô���topology�����ύ �����ύ�Ժ���ᷢ�ֲ�û�����С�
			 * ������kill��һЩtopology���ͷ���һЩslot��������topology�ͻ�ָ��������С�
			 */
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			// ָ��Ϊ����ģʽ����
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.killTopology("word-count");
			cluster.shutdown();
		}
	}
}
