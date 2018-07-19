package NLPpipeline;


import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class ReadTokenAction implements Function<String, Tuple2<String, Integer[]>> {

	/**
	 * read the tokenisation action for specific characters and creates a list of
	 * supposed action for each one
	 */
	private static final long serialVersionUID = -7679659559991351013L;

	@Override
	public Tuple2<String, Integer[]> call(String strLine) throws Exception {
		strLine = strLine.trim();
		String[] actions = strLine.trim().split("\\t");
		if (actions[0].toLowerCase().trim().equals("id")) {
			// header line
			return (new Tuple2<String, Integer[]>("header", new Integer[] { -1, -1, -1, -1 }));
		}
		Integer[] intActions = new Integer[4];
		for (int i = 2; i <= 5; i++) {
			if (actions[i].toLowerCase().equals("nothing")) {
				intActions[i - 2] = 0;
			} else if (actions[i].toLowerCase().equals("whitespace")) {
				intActions[i - 2] = 1;
			} else if (actions[i].toLowerCase().equals("delete")) {
				intActions[i - 2] = 2;
			} else {
				intActions[i - 2] = 3;
			}
		}
		return (new Tuple2<String, Integer[]>(actions[1].trim(), intActions));
	}

}

