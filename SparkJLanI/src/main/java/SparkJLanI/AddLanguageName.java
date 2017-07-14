package SparkJLanI;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AddLanguageName implements PairFunction<Tuple2<String, Double>, String, Tuple2<String, Double>> {
	private static final long serialVersionUID = 1L;
	private String languageName;
	
	public AddLanguageName(String lang) {
		languageName = lang;
	}
	
	public Tuple2<String, Tuple2<String, Double>> call(Tuple2<String, Double> t) throws Exception {
		return (new Tuple2<String, Tuple2<String, Double>>(t._1, new Tuple2<String, Double>(languageName, t._2)));
	}
}