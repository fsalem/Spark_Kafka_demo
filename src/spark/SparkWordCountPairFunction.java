package spark;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkWordCountPairFunction implements PairFunction<String, String, Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1021163620141181073L;

	public Tuple2<String, Integer> call(String t) throws Exception {
		return new Tuple2<String,Integer>(t,1);
	}

}
