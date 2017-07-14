package sentenceSegmentizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class ReadMetaData implements PairFlatMapFunction<String, String, Tuple2<String, Long>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<Tuple2<String, Tuple2<String, Long>>> call(String text) throws Exception {
		List<Tuple2<String, Tuple2<String, Long>>> lines = new ArrayList<Tuple2<String, Tuple2<String, Long>>>();
		String metaData = "";
		if (text.length() > 0) {
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
			String[] tempLines = text.split("\n");
			Tuple2<String, Long> tmp = new Tuple2<String, Long>("", 0L);
			for (int i = 0; i < tempLines.length; i++) {
				if (tempLines[i].length() > 0)
					tmp = new Tuple2<String, Long>(tempLines[i], (long) i);
				lines.add(new Tuple2<String, Tuple2<String, Long>>(metaData, tmp));
			}
		}
		return lines.iterator();
	}
}