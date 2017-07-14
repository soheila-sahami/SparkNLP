package SparkJLanI;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class seperateLanguages implements PairFunction<Tuple2<Long, Tuple2<Tuple2<String, Double>, Tuple3<String, Integer, Long>>>, String, Tuple3<String, Long, Double>> {

	@Override
	public Tuple2<String, Tuple3<String, Long, Double>> call(Tuple2<Long, Tuple2<Tuple2<String, Double>, Tuple3<String, Integer, Long>>> t) throws Exception {
		return new Tuple2<String, Tuple3<String, Long, Double>>(t._2._1()._1, new Tuple3<String, Long, Double>(t._2._2._1(), t._2._2._3(), t._2._1._2));
	}
}
