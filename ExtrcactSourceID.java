package NLPpipeline;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ExtrcactSourceID implements PairFunction<Tuple2<Long, String>, Long, Tuple2<Long, String>> {

	/**
	 * extracts metadata id from record id and add to record as key
	 */
	private static final long serialVersionUID = 831562394237521017L;
	private int offset;

	public ExtrcactSourceID(int offset1) {
		offset = offset1;
	}

	@Override
	public Tuple2<Long, Tuple2<Long, String>> call(Tuple2<Long, String> sentence) throws Exception {
		return new Tuple2<Long, Tuple2<Long, String>>((long) (sentence._1 / offset),
				new Tuple2<Long, String>(sentence._1, sentence._2));
	}
}
