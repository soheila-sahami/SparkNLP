package sentenceSegmentizer;

import org.apache.spark.api.java.function.Function;

public class UppercaseFirstLetterPreList implements Function<String, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String line) throws Exception {
		if (!Character.isUpperCase(line.charAt(0))) {
			return (line.substring(0, 1).toUpperCase() + line.substring(1, line.length()));
		}
		return line;
	}
}
