package kafka;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;



import org.apache.commons.collections.CollectionUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;


public class Consumertest {
	public static void main(String[] args) throws InterruptedException,
			UnsupportedEncodingException {
		Properties properties = new Properties();
		properties.put("zookeeper.connect",
				"JSNJ-IVR-SRV-I620G10-22:2181,JSNJ-IVR-SRV-I620G10-23:2181,JSNJ-IVR-SRV-I620G10-24:2181");
		properties.put("auto.commit.enable", "true");
		properties.put("auto.commit.interval.ms", "60000");
		properties.put("group.id", "test-group");
		ConsumerConfig consumerConfig = new ConsumerConfig(properties);
		ConsumerConnector javaConsumerConnector = Consumer
				.createJavaConsumerConnector(consumerConfig);
		// topic的过滤器
		Whitelist whitelist = new Whitelist("myTopic");
		List<KafkaStream<byte[], byte[]>> partitions = javaConsumerConnector
				.createMessageStreamsByFilter(whitelist);
		if (CollectionUtils.isEmpty(partitions)) {
			System.out.println("empty!");
			TimeUnit.SECONDS.sleep(1);
		}
		// 消费消息
		for (KafkaStream<byte[], byte[]> partition : partitions) {
			ConsumerIterator<byte[], byte[]> iterator = partition.iterator();
			while (iterator.hasNext()) {
				MessageAndMetadata<byte[], byte[]> next = iterator.next();
				System.out.println("partiton:" + next.partition() + " "
						+ "offset:" + next.offset() + "  get data:"
						+ new String(next.message(), "utf-8"));
			}
		}
	}
}
