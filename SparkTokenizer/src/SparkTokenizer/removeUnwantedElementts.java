package SparkTokenizer;

import org.apache.spark.api.java.function.Function;

public class removeUnwantedElementts implements Function<String, Boolean> {
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(String word) throws Exception {
		if (word.equals("") || word.equals("%^%") || word.equals("%$%") || word.equals("%N%") || word.equals("_TAB_") || word.equals("%_%") || word.equals("."))
			return (false);
		return (true);
	}
}
