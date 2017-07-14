package SparkJLanI;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple5;

public class ConvertKeyColumn implements Function <Tuple2<String, Tuple2<Tuple2<Long, Integer>, Tuple2<String, Double>>>, Tuple5<String, Long, Integer,String, Double>>{
	private static final long serialVersionUID = 1L;
	public Tuple5<String, Long, Integer,String, Double> call(Tuple2<String, Tuple2<Tuple2<Long, Integer>, Tuple2<String, Double>>> t)
			throws Exception {
		String word = t._1;
		Long sentenceId = t._2._1._1;
		Integer numOfWords=t._2._1._2;
		String langName = t._2._2._1;
		Double wordProb =t._2._2._2;
		return new Tuple5<String, Long, Integer,String, Double>(word,sentenceId,numOfWords,langName,wordProb);
	}
}
