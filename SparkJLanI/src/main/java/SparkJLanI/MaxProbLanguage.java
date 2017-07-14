package SparkJLanI;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class MaxProbLanguage implements Function2<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>> {
	@Override
	public Tuple2<String, Double> call(Tuple2<String, Double> v1, Tuple2<String, Double> v2) throws Exception {
		if (v1._2() > v2._2())
			return (v1);
		else
			return (v2);
	}

}
