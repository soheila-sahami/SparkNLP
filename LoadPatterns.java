package NLPpipeline;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class LoadPatterns implements Function<String, Tuple2<Pattern, Boolean>> {

	/**
	 * check and compile each rule
	 */
	private static final long serialVersionUID = -701569116765947138L;
	private final static Logger logger = Logger.getLogger(LoadPatterns.class.getName());

	public Tuple2<Pattern, Boolean> call(String prePattern) throws Exception {
		if (!prePattern.startsWith("#") && !prePattern.isEmpty()) {
			if (!prePattern.startsWith("- ") && !prePattern.startsWith("+ ")) {
				logger.error("Unable to parse line " + prePattern
						+ " in postBoundaryRules-file. Patterns need to specify a decision using '+ '/'- ' in front of the pattern.");
				logger.debug(prePattern);
			} else {
				Tuple2<Pattern, Boolean> compiledPattern = new Tuple2<Pattern, Boolean>(
						Pattern.compile(prePattern.substring(2, prePattern.length())), prePattern.startsWith("+ "));
				return compiledPattern;
			}
		}
		return new Tuple2<Pattern, Boolean>(null, false);
	}

}