package NLPpipeline;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class SplitDocumentToSentences implements PairFlatMapFunction<Tuple2<String, Long>, Long, String> {
	/**
	 * if document has no metadata, splits the documents on "\n" and creates an Id
	 * for each part.
	 */
	private static final long serialVersionUID = 8117955895018035190L;
	long offset;

	public SplitDocumentToSentences(long offset1) {
		offset = offset1;
	}

	@Override
	public Iterator<Tuple2<Long, String>> call(Tuple2<String, Long> document) throws Exception {

		String[] docs = document._1.split("\n");
		ArrayList<Tuple2<Long, String>> splitted = new ArrayList<Tuple2<Long, String>>();

		for (int i = 0; i < docs.length; i++) {
			splitted.add(new Tuple2<Long, String>((long) ((document._2 * offset) + i), docs[i]));
		}
		return splitted.iterator();
	}
}
