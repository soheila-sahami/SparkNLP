package de.uni_leipzig.informatik.asv.SparkPosTagger;

import org.apache.spark.api.java.function.Function;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import scala.Tuple2;

public class stanfordTagger implements Function<Tuple2<String, Long>, Tuple2<String, Long>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, Long> call(Tuple2<String, Long> sentence) throws Exception {
		MaxentTagger tagger = new MaxentTagger("taggers/wsj-0-18-bidirectional-distsim.tagger");
		String tagged = tagger.tagString(sentence._1);
		return new Tuple2<String, Long>(tagged, sentence._2);
	}

}
