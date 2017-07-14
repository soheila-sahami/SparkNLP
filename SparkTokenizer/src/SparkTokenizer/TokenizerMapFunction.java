package SparkTokenizer;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

class TokenizerMapFunction implements Function<Tuple2<String, Long>, Tuple2<String, Long>> {
	private static final long serialVersionUID = 1L;
	Tokenizer tokenizer = new CharacterBasedWordTokenizerImpl();

	public TokenizerMapFunction(Tokenizer tokenizerInstance) {
		tokenizer = tokenizerInstance;
	}

	@Override
	public Tuple2<String, Long> call(Tuple2<String, Long> strLine) throws Exception {
		String tokenizedSentence = tokenizer.execute(strLine._1);
		return (new Tuple2<String, Long>(tokenizedSentence, strLine._2));
	}

}
