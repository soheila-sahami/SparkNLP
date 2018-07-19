package NLPpipeline;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ConcatAndSortDocs implements PairFunction<Tuple2<Long, Iterable<Tuple2<Long, String>>>, Long, String> {

	/**
	 * sort and concatenates all of the sentences which belongs to one document to
	 * recreate the document before writing to output files
	 */
	private static final long serialVersionUID = 2151212693512260664L;
	String seperator = "\n";

	public ConcatAndSortDocs(String seperator1) {
		seperator = seperator1;
	}
	@Override
	public Tuple2<Long, String> call(Tuple2<Long, Iterable<Tuple2<Long, String>>> sentences) throws Exception {
		HashMap<Long, String> srcHashMap = new HashMap<Long, String>();
		sentences._2.forEach(new Consumer<Tuple2<Long, String>>() {
			@Override
			public void accept(Tuple2<Long, String> lines) {
				srcHashMap.put(lines._1, lines._2);
			}
		});
		String str = srcHashMap.entrySet().stream()
				.sorted(Map.Entry.comparingByKey())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue,
						LinkedHashMap::new))
				.entrySet().stream()
				.map(x -> x.getValue())
				.collect(Collectors.joining(seperator));
		return new Tuple2<Long, String>(sentences._1, str);
	}
}