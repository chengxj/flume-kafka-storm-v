package kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class Partitionertest implements Partitioner {
	public Partitionertest(VerifiableProperties props) {
	}

	@Override
	public int partition(Object obj, int a_numPartitions) {
		// String key = obj.toString();
		// return Integer.parseInt(key)% a_numPartitions;
		return obj.hashCode() % a_numPartitions;
	}
}
