package spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import utils.PropertiesStack;

public class SparkKafkaPrint implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// static Logger logger = Logger.getLogger(SparkWordCount.class);
	// static {
	// logger.setLevel(Level.ERROR);
	// }
	// private final String LOCAL_SPARK = "spark://ws-37:7077";
	// private final String SERVER_SPARK = "spark://stream-demo.novalocal:7077";
	// private final String SERVER_SPARK = "spark://192.168.74.30:7077";

	public void run() {

		System.setProperty("spark.hadoop.dfs.replication", "2");

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkConf conf = new SparkConf()
				.setAppName("WindowingKafkaWordCountWithFaultTolerance");
		conf.set("spark.master", PropertiesStack.getProperty("spark.master"));
		conf.set("spark.executor.memory",
				PropertiesStack.getProperty("spark.executor.memory"));
		conf.set("spark.driver.memory",
				PropertiesStack.getProperty("spark.driver.memory"));
		conf.set("spark.driver.maxResultSize",
				PropertiesStack.getProperty("spark.driver.maxResultSize"));
		// .setAppName("WindowingKafkaWordCountWithoutFaultTolerance");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10));

		HashSet<String> topicsSet = new HashSet<String>(
				Arrays.asList(PropertiesStack.getKafkaTopic()));

		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list",
				PropertiesStack.getKafkaBootstrapServers());
		kafkaParams.put("zookeeper.connect",
				PropertiesStack.getZookeeperConnect());
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("group.id", PropertiesStack.getKafkaGroupId());
		kafkaParams.put("auto.commit.enable", "false");

		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(PropertiesStack.getKafkaTopic(), 1);
//		Map<kafka.common.TopicAndPartition, java.lang.Long> fromOffsets = new HashMap<>();
//		fromOffsets.put(new TopicAndPartition(PropertiesStack.getKafkaTopic(),
//				1), 1000L);
		// Create direct kafka stream with brokers and topics
//		JavaInputDStream<String> messages = KafkaUtils
//				.createDirectStream(
//						jssc,
//						String.class,
//						String.class,
//						StringDecoder.class,
//						StringDecoder.class,
//						String.class,
//						kafkaParams,
//						fromOffsets,
//						new Function<kafka.message.MessageAndMetadata<String, String>, String>() {
//							@Override
//							public String call(
//									MessageAndMetadata<String, String> v1)
//									throws Exception {
//								return v1.message();
//							}
//						});
				JavaPairInputDStream<String,String> messages = KafkaUtils
		 .createDirectStream(jssc, String.class, String.class,
		 StringDecoder.class, StringDecoder.class, kafkaParams,
		 topicsSet);
		messages.count().print();
		// .createStream(jssc, PropertiesStack.getZookeeperConnect(),
		// PropertiesStack.getKafkaGroupId(), topicMap);

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	public static void main(String[] args) {
		System.out.println("program started at " + System.currentTimeMillis());
		new SparkKafkaPrint().run();

	}
}
