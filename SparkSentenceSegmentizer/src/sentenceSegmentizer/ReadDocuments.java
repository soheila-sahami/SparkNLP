package sentenceSegmentizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class ReadDocuments implements PairFlatMapFunction<String, String, Tuple2<String, Long>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<Tuple2<String, Tuple2<String, Long>>> call(String text) throws Exception {
		List<Tuple2<String, Tuple2<String, Long>>> lines = new ArrayList<Tuple2<String, Tuple2<String, Long>>>();
		String metaData = "";
		String[] tempLines = text.split("\n");
		Tuple2<String, Long> tmp = new Tuple2<String, Long>("", 0L);
		for (int i = 0; i < tempLines.length; i++) {
			if (tempLines[i].length() > 0)
				tmp = new Tuple2<String, Long>(tempLines[i], (long) i);
			lines.add(new Tuple2<String, Tuple2<String, Long>>(metaData, tmp));
		}
		return lines.iterator();
	}
}