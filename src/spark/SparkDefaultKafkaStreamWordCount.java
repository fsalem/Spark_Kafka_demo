package spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import kafka.serializer.StringDecoder;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import utils.PropertiesStack;
import fi.aalto.spark.kafka.writer.JavaDStreamKafkaWriter;
import fi.aalto.spark.kafka.writer.JavaDStreamKafkaWriterFactory;

public class SparkDefaultKafkaStreamWordCount implements Serializable {
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
				.setAppName("WindowingDefaultKafkaWordCount");
		conf.set("spark.master", PropertiesStack.getProperty("spark.master"));
		conf.set("spark.executor.memory",
				PropertiesStack.getProperty("spark.executor.memory"));
		conf.set("spark.driver.memory",
				PropertiesStack.getProperty("spark.driver.memory"));
		conf.set("spark.driver.maxResultSize",
				PropertiesStack.getProperty("spark.driver.maxResultSize"));
		//conf.set("spark.streaming.backpressure.enabled", "true");
		
		if (PropertiesStack.getReceiverMaxRate() != null) {
			System.out.println("spark.streaming.receiver.maxRate="
					+ PropertiesStack.getReceiverMaxRate());
			conf.set("spark.streaming.receiver.maxRate",
					PropertiesStack.getReceiverMaxRate());
		}
		
		if (PropertiesStack.getReceiverMaxRatePerPartition() != null) {
			System.out.println("spark.streaming.receiver.maxRatePerPartition="
					+ PropertiesStack.getReceiverMaxRatePerPartition());
			conf.set("spark.streaming.kafka.maxRatePerPartition",
					PropertiesStack.getReceiverMaxRatePerPartition());
		}
		
		if (PropertiesStack.getKafkaMaxRetries() != null) {
			conf.set("spark.streaming.kafka.maxRetries",
					PropertiesStack.getKafkaMaxRetries());
		}
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10));
		if (PropertiesStack.isCheckpointEnabled()) {
			jssc.checkpoint(PropertiesStack.getProperty("spark.checkpoint.dir"));
		}

		HashSet<String> topicsSet = new HashSet<String>(
				Arrays.asList(PropertiesStack.getKafkaTopic()));

		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list",
				PropertiesStack.getKafkaBootstrapServers());
		kafkaParams.put("zookeeper.connect",
				PropertiesStack.getZookeeperConnect());
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("group.id", PropertiesStack.getKafkaGroupId());
//		kafkaParams.put("auto.commit.enable", "false");

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(jssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicsSet);
		//messages.count().print();
		JavaDStream<Tuple2<String, String>> wordCount = messages
				.flatMap(new SparkLineSplitterFunction())
				.mapToPair(new SparkWordCountPairFunction())
				// .reduceByKey((a, b) -> a + b)
				.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(10))
				.map(new Function<Tuple2<String, Integer>, Tuple2<String, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<String, Integer> tuple)
							throws Exception {
						return new Tuple2<String, String>(null, tuple._1() + "\t" + tuple._2());
					}
				});

		if (PropertiesStack.isCheckpointEnabled()) {
			wordCount.checkpoint(Durations.milliseconds(PropertiesStack
					.getCheckpointDuration()));
		}

		Properties props = new Properties();
		props.put("bootstrap.servers",
				PropertiesStack.getKafkaBootstrapServers());
//		props.put("acks", "all");
//		props.put("retries", Integer.MAX_VALUE);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
//		props.setProperty("block.on.buffer.full", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("group.id", PropertiesStack.getKafkaGroupId());
//		props.put("auto.commit.enable", "false");
		// props.put("zookeeper.connect",
		// PropertiesStack.getZookeeperConnect());
		// props.put("metadata.broker.list",
		// PropertiesStack.getKafkaBootstrapServers());
		// props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		// props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		JavaDStreamKafkaWriter writer = JavaDStreamKafkaWriterFactory
				.getKafkaWriter(wordCount, props,
						PropertiesStack.getResultKafkaTopic(), PropertiesStack.isKafkaAsync());
		writer.writeToKafka();
		// JavaDStreamKafkaWriter<String> writer = JavaDStreamKafkaWriterFactory
		// .fromJavaDStream(wordCount);
		// writer.writeToKafka(props, new KafkaProducerProcessingFunction());
		// wordCount.print();
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	public static void main(String[] args) {
		System.out.println("program started at " + System.currentTimeMillis());
		new SparkDefaultKafkaStreamWordCount().run();

	}
}
