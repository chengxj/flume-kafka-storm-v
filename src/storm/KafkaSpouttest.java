package storm;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class KafkaSpouttest implements IRichSpout {

	private SpoutOutputCollector collector;
	private ConsumerConnector consumer;
	private String topic;

	public KafkaSpouttest() {
	}

	public KafkaSpouttest(String topic) {
		this.topic = topic;
	}

	@Override
	public void nextTuple() {
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void activate() {

		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig());

		Map<String, Integer> topickMap = new HashMap<String, Integer>();
		topickMap.put(topic, 1);

		System.out.println("*********Results********topic:" + topic);

		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer
				.createMessageStreams(topickMap);
		KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			String value = new String(it.next().message());
			SimpleDateFormat formatter = new SimpleDateFormat(
					"yyyy年MM月dd日 HH:mm:ss SSS");
			Date curDate = new Date(System.currentTimeMillis());// 获取当前时间
			String str = formatter.format(curDate);

			System.out.println("storm接收到来自kafka的消息------->" + value);

			collector.emit(new Values(value, 1, str), value);
		}
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		// 设置zookeeper的链接地址
		props.put(
				"zookeeper.connect",
				"JSNJ-IVR-SRV-I620G10-22:2181,JSNJ-IVR-SRV-I620G10-23:2181,JSNJ-IVR-SRV-I620G10-24:2181");
		// 设置group id
		props.put("group.id", "1");
		// kafka的group 消费记录是保存在zookeeper上的, 但这个信息在zookeeper上不是实时更新的, 需要有个间隔时间更新
		props.put("auto.commit.interval.ms", "1000");
		props.put("zookeeper.session.timeout.ms", "10000");
		return new ConsumerConfig(props);
	}

	@Override
	public void close() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "id", "time"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		System.out.println("getComponentConfiguration被调用");
		topic = "myTopic";
		return null;
	}
}
