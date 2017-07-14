package SparkJLanI;

import java.util.Iterator;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

public class ProbabilityOfSentence implements Function<Tuple2<Tuple3<Long, String, Integer>, Iterable<Tuple2<String, Double>>>, Tuple3<Long, String, Double>> {
	private static final long serialVersionUID = 1L;
	private double minProb;

	public ProbabilityOfSentence(double minimumProb) {
		minProb = minimumProb;
	}

	@Override
	public Tuple3<Long, String, Double> call(Tuple2<Tuple3<Long, String, Integer>, Iterable<Tuple2<String, Double>>> word) throws Exception {
		int detectedWordCount = 0;
		double probabilty = 0.0D;
		int numOfTotalWordInSent = word._1._3();

		Iterator<Tuple2<String, Double>> iter = word._2.iterator();
		while (iter.hasNext()) {
			probabilty += iter.next()._2;
			detectedWordCount++;
		}
		if (detectedWordCount < numOfTotalWordInSent) {
			probabilty = probabilty + (minProb * (numOfTotalWordInSent - detectedWordCount));
		}
		probabilty = probabilty / numOfTotalWordInSent;
		return new Tuple3<Long, String, Double>(word._1._1(), word._1._2(), probabilty);
	}
}
