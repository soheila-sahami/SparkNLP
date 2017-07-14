package SparkTokenizer;

import org.apache.spark.api.java.function.Function;

public class readTokenizerChar implements Function<String, String> {

	@Override
	public String call(String strLine) throws Exception {
		String strSplit[] = strLine.split("\t");
		String strTokenisationCharacter = "";
		if (strSplit.length == 2)
			strTokenisationCharacter = strSplit[1].trim();
		return strTokenisationCharacter;
	}

}
