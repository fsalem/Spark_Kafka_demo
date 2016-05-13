package spark;

import kafka.producer.KeyedMessage;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import utils.PropertiesStack;

public class KafkaProducerProcessingFunctionWithIterable
		implements
		Function<Tuple2<String, Iterable<Integer>>, KeyedMessage<String, byte[]>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9173024409102537588L;

	@Override
	public KeyedMessage<String, byte[]> call(
			Tuple2<String, Iterable<Integer>> tuple) throws Exception {
		//Logger.getRootLogger().log(Priority.INFO, new Date().getTime());
		Integer count = 0;
		for (int i : tuple._2())
			count += i;
		return new KeyedMessage<String, byte[]>(
				PropertiesStack.getResultKafkaTopic(), null,
				(tuple._1() + "\t" + count.toString()).getBytes());
	}

}
