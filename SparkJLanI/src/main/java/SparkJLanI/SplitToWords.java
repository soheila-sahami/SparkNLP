package SparkJLanI;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import scala.Tuple3;

public class SplitToWords implements PairFlatMapFunction<Tuple2<Long, Tuple3<String, Integer, Long>>, String, Tuple2<Long, Integer>> {
	private static final long serialVersionUID = 1L;
	private String wordsSpilter;

	public SplitToWords(String userWordsSpilter) {
		wordsSpilter = userWordsSpilter;
	}

	@Override
	public Iterator<Tuple2<String, Tuple2<Long, Integer>>> call(Tuple2<Long, Tuple3<String, Integer, Long>> s) {
		List<Tuple2<String, Tuple2<Long, Integer>>> word = new ArrayList<Tuple2<String, Tuple2<Long, Integer>>>();
		String[] wordList = s._2._1().split(wordsSpilter);
		for (String w : wordList) {
			word.add(new Tuple2<String, Tuple2<Long, Integer>>(w, new Tuple2<Long, Integer>(s._1, s._2._2())));
		}
		return (word).iterator();
	}
}