package NLPpipeline;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ReadDocumentMetaData implements PairFunction<Tuple2<String, Long>, Long, Tuple2<String, String>> {

	/**
	 * gets a document with metadata (has source tag ) and split it to
	 * metadata(source information) and text.
	 * If metadata is not clear (no end tag for source), creates error and ignores
	 * the documents
	 */
	private static final long serialVersionUID = 2488127315706596858L;
	private final static Logger logger = Logger.getLogger(ReadDocumentMetaData.class.getName());

	@Override
	public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, Long> document) throws Exception {

		String metaData = "";
		String text = "";
		Tuple2<Long, Tuple2<String, String>> splitted = new Tuple2<Long, Tuple2<String, String>>(-1L,
				new Tuple2<String, String>(metaData, text));

		if (document._1.length() > 0) {
			int endPos = document._1.indexOf("</source>");
			if (endPos == -1) {// has no metadata end tag
				logger.error("End of metdadata is not clear! Ignore the text.\n");
				logger.debug(document._1);
				text = ""; //
			} else {// has start and end tag
				metaData = "<source>" + document._1.substring(0, document._1.indexOf("</source>") + 9);
				if (endPos + 9 < document._1.length()) // has text after end tag
					text = document._1.substring(document._1.indexOf("</source>") + 9, document._1.length());
				else
					text = "";
			}
			splitted = new Tuple2<Long, Tuple2<String, String>>(document._2,
					new Tuple2<String, String>(metaData, text));
		}
		return splitted;
	}
}