package NLPpipeline;

import org.apache.spark.api.java.function.Function;

public class RemoveUnwantedElementts implements Function<String, Boolean> {
	/**
	 * filters specified characters from fixed tokens list
	 */
	private static final long serialVersionUID = 8419401567264709614L;

	@Override
	public Boolean call(String word) throws Exception {
		if (word.equals("") || word.equals("%^%") || word.equals("%$%") || word.equals("%N%") || word.equals("_TAB_")
				|| word.equals("%_%") || word.equals("."))
			return (false);
		return (true);
	}
}
