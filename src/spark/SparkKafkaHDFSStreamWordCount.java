package spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import utils.PropertiesStack;

public class SparkKafkaHDFSStreamWordCount implements Serializable {
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
		kafkaParams.put("auto.commit.enable", "false");

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(jssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicsSet);

		// using checkpoint
		// JavaPairDStream<String, Iterable<Integer>> wordCount = messages
		// .flatMap(new SparkLineSplitterFunction())
		// .mapToPair(new SparkWordCountPairFunction())
		// .groupByKeyAndWindow(Durations.seconds(10),Durations.seconds(10))
		// .reduceByKeyAndWindow(new SparkReduceFunction2(),
		// new SparkReduceFunction2(), Durations.seconds(10),
		// Durations.seconds(10));

		JavaPairDStream<String, Iterable<Integer>> wordCount = messages
				.flatMap(new SparkLineSplitterFunction())
				.mapToPair(new SparkWordCountPairFunction())
				.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(10))
				.groupByKeyAndWindow(Durations.seconds(10));
		// wordCount
		// .foreachRDD(new VoidFunction<JavaPairRDD<String,
		// Iterable<Integer>>>() {
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public void call(JavaPairRDD<String, Iterable<Integer>> t)
		// throws Exception {
		// t.foreachPartition(new VoidFunction<Iterator<Tuple2<String,
		// Iterable<Integer>>>>() {
		// private static final long serialVersionUID = 1L;
		//
		// public void call(
		// Iterator<scala.Tuple2<String, java.lang.Iterable<Integer>>> iterator)
		// throws Exception {
		// if (iterator == null)
		// return;
		// KafkaProducerObject kafkaProducerObject = new KafkaProducerObject();
		// scala.Tuple2<String, java.lang.Iterable<Integer>> message;
		// Integer sum;
		// while (iterator.hasNext()) {
		// message = iterator.next();
		// sum = 0;
		// while (message._2.iterator().hasNext()) {
		// sum += message._2.iterator().next();
		// }
		// kafkaProducerObject.send(message._1()
		// + "\t" + sum.toString(),
		// PropertiesStack
		// .getResultKafkaTopic());
		// }
		// kafkaProducerObject.close();
		// };
		// });
		// }
		// });
		if (PropertiesStack.isCheckpointEnabled()){
			wordCount.checkpoint(Durations.milliseconds(PropertiesStack
					.getCheckpointDuration()));
		}
		
		JavaPairDStream<Text, IntWritable> hadoopWordCount = wordCount
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, Text, IntWritable>() {
					public scala.Tuple2<Text, IntWritable> call(
							scala.Tuple2<String, java.lang.Iterable<Integer>> tuple)
							throws Exception {
						Integer sum = 0;
						Iterator<Integer> it = tuple._2.iterator();
						while (it.hasNext()) {
							sum += it.next();
						}
						return new Tuple2<Text, IntWritable>(
								new Text(tuple._1), new IntWritable(sum));
					};
				});
		hadoopWordCount.saveAsHadoopFiles(
				PropertiesStack.getProperty("spark.hdfs.sink"),
				"sparkWordCount.windowing", Text.class, IntWritable.class,
				TextOutputFormat.class);
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	public static void main(String[] args) {
		System.out.println("program started at " + System.currentTimeMillis());
		new SparkKafkaHDFSStreamWordCount().run();

	}
}
