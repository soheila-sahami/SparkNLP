package sentenceSegmentizer;

import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class removeNullRules implements Function<Tuple2<Pattern, Boolean>, Boolean> {

	@Override
	public Boolean call(Tuple2<Pattern, Boolean> v1) throws Exception {
		if (v1._1() == null)
			return false;
		return true;
	}

}
