package spark;

import java.util.Date;

import kafka.producer.KeyedMessage;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.spark.api.java.function.Function;

import utils.PropertiesStack;

public class KafkaProducerProcessingFunction implements
		Function<String, KeyedMessage<String, byte[]>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9173024409102537588L;

	@Override
	public KeyedMessage<String, byte[]> call(String tuple) throws Exception {
		Logger.getRootLogger().log(Priority.INFO, new Date().getTime());
		return new KeyedMessage<String, byte[]>(
				PropertiesStack.getResultKafkaTopic(), null, (tuple).getBytes());
	}

}
