package spark;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class SparkLineSplitterFunction implements FlatMapFunction<Tuple2<String, String>,String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public Iterable<String> call(Tuple2<String, String> t)
			throws Exception {
		return Arrays.asList(t._2.toLowerCase().split("\\W+"));
	}
	
	
}
