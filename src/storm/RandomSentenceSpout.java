package storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {

	/**
	 * �����������ݵĹ�����
	 */
	SpoutOutputCollector _collector;
	Random _rand;

	/**
	 * �����ʼ��collector
	 */
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	/**
	 * �÷�������SpoutTracker���б�����ÿ����һ�ξͿ�����storm��Ⱥ�з���һ�����ݣ�һ��tupleԪ�飩 �÷����ᱻ��ͣ�ĵ���
	 * ������Ϣ��������Ӧ����Ϣ
	 */
	@Override
	public void nextTuple() {

		// ģ��ȴ�100ms
		Utils.sleep(100);

		// �����������
		String[] sentences = new String[] { "the cow jumped over the moon",
				"an apple a day keeps the doctor away",
				"four score and seven years ago",
				"snow white and the seven dwarfs", "i am at two with nature" };
		String sentence = sentences[_rand.nextInt(sentences.length)];
		// ���÷��䷽��
		_collector.emit(new Values(sentence));
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	/**
	 * ���ﶨ���ֶ�id����id�ڼ�ģʽ��û���ô������ڰ����ֶη����ģʽ���кܴ���ô��� ��declarer�����кܴ����ã����ǻ����Ե���
	 * declarer.declareStream(); ������stramId����id������������ ���Ӹ��ӵ������˽ṹ
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
