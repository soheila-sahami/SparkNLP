package SparkImp.SparkSentenceCleaner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class createMetaData implements PairFlatMapFunction<String, String, String> {

	private static final long serialVersionUID = 1L;
	boolean metaDataIncluded = false;

	public createMetaData(boolean hasMetadata) {
		metaDataIncluded = hasMetadata;
	}

	@Override
	public Iterator<Tuple2<String, String>> call(String text) throws Exception {
		List<Tuple2<String, String>> lines = new ArrayList<Tuple2<String, String>>();
		String metaData = "";
		if (text.length() > 0) {
			if (metaDataIncluded) {
				// int startPos = text.indexOf("<source>");
				int endPos = text.indexOf("</source>");
				if (endPos == -1) {// has no metadata end tag
					System.out.println("End of metdadata is not clear!\n");
					text = ""; //
				} else {// has start and end tag
					metaData = "<source>" + text.substring(0, text.indexOf("</source>") + 9);
					if (endPos + 9 < text.length()) // has text after end tag
						text = text.substring(text.indexOf("</source>") + 9, text.length());
					else
						text = "";
				}
			}
			String[] tempLines = text.split("\n");
			for (int i = 0; i < tempLines.length; i++) {
				if (tempLines[i].length() > 0)
					lines.add(new Tuple2<String, String>(metaData, tempLines[i]));
			}
		}
		return lines.iterator();
	}
}
