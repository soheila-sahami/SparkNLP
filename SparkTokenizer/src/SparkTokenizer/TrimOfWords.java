package SparkTokenizer;

import org.apache.spark.api.java.function.Function;

public class TrimOfWords implements Function<String, String> {
	private static final long serialVersionUID = 1L;
	private int addDot = 0;

	public TrimOfWords(int addDotToWord) {
		addDot = addDotToWord;
	}

	@Override
	public String call(String word) throws Exception {
		if (addDot == 1)
			return word.trim() + ".";
		else
			return word.trim();
	}

}
