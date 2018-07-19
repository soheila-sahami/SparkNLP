package NLPpipeline;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class CreateDocumentSourceIndex
		implements PairFlatMapFunction<Tuple2<Long, Tuple2<String, String>>, Long, String> {

	/**
	 * extract the metadata of documents (source information) and its ID to create
	 * the metadata record set
	 */
	private static final long serialVersionUID = -6472677297402736599L;

	@Override
	public Iterator<Tuple2<Long, String>> call(Tuple2<Long, Tuple2<String, String>> line) throws Exception {
		return Arrays.asList(new Tuple2<Long, String>(line._1, line._2._1)).iterator();
	}
}