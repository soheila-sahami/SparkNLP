package sentenceSegmentizer;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

public class LoadAbbrevations implements FlatMapFunction<String, String> {
	private static final long serialVersionUID = 1L;
	boolean autoUppercaseFirstLetter;

	public LoadAbbrevations(boolean autoUppercaseFirstLetterPreList) {
		autoUppercaseFirstLetter = autoUppercaseFirstLetterPreList;
	}

	@Override
	public Iterator call(String line) throws Exception {
		String reversedAbbrevations[] = { new StringBuilder(line).reverse().toString(), null };
		if (autoUppercaseFirstLetter && !Character.isUpperCase(line.charAt(0))) {
			String firstLetterUpperCaseLine = line.substring(0, 1).toUpperCase() + line.substring(1, line.length());
			reversedAbbrevations[1] = new StringBuilder(firstLetterUpperCaseLine).reverse().toString();
		}
		return Arrays.asList(reversedAbbrevations).iterator();
	}
}
