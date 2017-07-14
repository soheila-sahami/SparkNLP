package SparkTokenizer;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class RemoveHeader implements Function<Tuple2<String, Integer[]>, Boolean> {
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(Tuple2<String, Integer[]> strLine) throws Exception {
		if (strLine._1.equals("header"))
			return false;
		else
			return true;
	}
}
