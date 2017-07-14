package SparkJLanI;

import org.apache.spark.api.java.function.Function;

public class CalculateWordProbability implements Function<Double, Double> {
	private static final long serialVersionUID = 1L;
	private Double totalFreq;
	
	public CalculateWordProbability(Double total) {
		totalFreq = total;
	}
	
	public Double call(Double iteration) throws Exception {
		return (iteration / totalFreq);
	}
}
