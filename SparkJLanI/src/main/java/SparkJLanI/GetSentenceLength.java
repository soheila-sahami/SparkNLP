package SparkJLanI;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class GetSentenceLength implements PairFunction<Tuple2<Tuple2<String, Long>, Long>, Long, Tuple3<String, Integer, Long>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Long, Tuple3<String, Integer, Long>> call(Tuple2<Tuple2<String, Long>, Long> t) throws Exception {
		return (new Tuple2<Long, Tuple3<String, Integer, Long>>(t._2, new Tuple3<String, Integer, Long>(t._1._1, t._1._1.split(" ").length, t._1._2)));
	}
}