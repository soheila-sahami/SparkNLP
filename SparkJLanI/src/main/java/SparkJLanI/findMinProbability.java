package SparkJLanI;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple2;

public class findMinProbability implements Comparator<Tuple2<String, Tuple2<String, Double>>>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<String, Tuple2<String, Double>> word1, Tuple2<String, Tuple2<String, Double>> word2) {
		if (word1._2._2 > word2._2._2)
			return 1;
		else
			return 0;
	}

}
