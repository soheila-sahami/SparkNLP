package sentenceSegmentizer;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class createSourceIndex implements FlatMapFunction<Tuple2<String, Tuple2<String, Long>>, String> {
	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<String> call(Tuple2<String, Tuple2<String, Long>> line) throws Exception {
		return Arrays.asList(line._1).iterator();
	}
}