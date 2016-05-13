package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class SparkGroupedLinesFlatMapFunction implements FlatMapFunction<Tuple2<String, Iterable<String>>,Tuple2<String, String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -223429148907202180L;

	
	@Override
	public Iterable<Tuple2<String, String>>call(
			Tuple2<String, Iterable<String>> value) throws Exception {
		
		ArrayList<Tuple2<String, String>> words = new ArrayList<>();
		
		ArrayList<String> tmpWords = new ArrayList<>();
		Map<String, Integer> wordCountMap = new HashMap<>();
		for (String line:value._2()){
			tmpWords.addAll(Arrays.asList(line.toLowerCase().split("\\W+")));
			for (String word:tmpWords){
				if (wordCountMap.containsKey(word)){
					wordCountMap.put(word, wordCountMap.get(word)+1);
				}else{
					wordCountMap.put(word, 1);
				}
			}
			tmpWords.clear();
		}
		
		Set<String> groupWords = wordCountMap.keySet();
		for (String word:groupWords){
			words.add(new Tuple2<String, String>(value._1(), word+"\t"+wordCountMap.get(word)));
		}
		
		return words;
	}
	
}
