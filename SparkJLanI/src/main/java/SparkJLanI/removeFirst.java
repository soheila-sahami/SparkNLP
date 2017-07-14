package SparkJLanI;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class removeFirst implements Function<Tuple2<String, Double>, Boolean> {
	private static final long serialVersionUID = 1L;
	
	public Boolean call(Tuple2<String, Double> v1) throws Exception {
		if (v1._1.isEmpty() || v1._1 == null)
			return false;
		else
			return true;
	}
}