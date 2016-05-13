package spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.Function2;

public class SparkReduceFunction2 implements
		Function2<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Iterable<Integer> call(Iterable<Integer> v1, Iterable<Integer> v2)
			throws Exception {
		List<Integer> combine = new ArrayList<Integer>();
		Iterator<Integer> it1 = v1.iterator();
		Iterator<Integer> it2 = v2.iterator();
		while (it1.hasNext())
			combine.add(it1.next());
		while (it2.hasNext())
			combine.add(it2.next());

		return combine;
	}

}
