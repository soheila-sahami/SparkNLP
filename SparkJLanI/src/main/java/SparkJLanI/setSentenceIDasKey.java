package SparkJLanI;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

public class setSentenceIDasKey implements PairFunction<Tuple5<String, Long, Integer, String, Double>, Tuple3<Long, String, Integer>, Tuple2<String, Double>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Tuple3<Long, String, Integer>, Tuple2<String, Double>> call(Tuple5<String, Long, Integer, String, Double> t) throws Exception {
		return new Tuple2<Tuple3<Long, String, Integer>, Tuple2<String, Double>>(new Tuple3<Long, String, Integer>(t._2(), t._4(), t._3()),
				new Tuple2<String, Double>(t._1(), t._5()));
	}
}