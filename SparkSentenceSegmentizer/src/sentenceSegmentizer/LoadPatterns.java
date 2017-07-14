package sentenceSegmentizer;

import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class LoadPatterns implements Function<String, Tuple2<Pattern, Boolean>> {

	public Tuple2<Pattern, Boolean> call(String prePattern) throws Exception {
		if (!prePattern.startsWith("#") && !prePattern.isEmpty()) {
			if (!prePattern.startsWith("- ") && !prePattern.startsWith("+ ")) {
				System.err.println("Unable to parse line " + prePattern
						+ " in postBoundaryRules-file. Patterns need to specify a decision using '+ '/'- ' in front of the pattern.");
			} else {
				Tuple2<Pattern, Boolean> compiledPattern = new Tuple2<Pattern, Boolean>(
						Pattern.compile(prePattern.substring(2, prePattern.length())), prePattern.startsWith("+ "));
				return compiledPattern;
			}
		}
		return new Tuple2<Pattern, Boolean>(null, false);
	}

}