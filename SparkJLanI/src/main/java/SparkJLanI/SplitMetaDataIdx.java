package SparkJLanI;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class SplitMetaDataIdx implements Function<String, Tuple2<String, Long>> {

	@Override
	public Tuple2<String, Long> call(String line) throws Exception {
		int stringLength = Integer.valueOf(line.substring(1, line.indexOf(",")));
		String text = line.substring(line.indexOf(",") + 1, line.indexOf(",") + stringLength + 1);
		String metaData = line.substring(line.indexOf(",") + stringLength + 2, line.length() - 1);
		Long metaDataIdx = Long.valueOf(metaData);
		return (new Tuple2<String, Long>(text, metaDataIdx));
		// String text = line.substring(1, line.indexOf(",<source>"));
		// String metaData = line.substring(line.indexOf("<source>"),
		// line.length() - 2);
		// return (new Tuple2<String, String>(text, metaData));
	}
}