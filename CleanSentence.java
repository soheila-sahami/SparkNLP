package NLPpipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class CleanSentence implements PairFlatMapFunction<Tuple2<Long, String>, Long, String> {
	/**
	 * apply the cleaning function on all records of recored set (sentences).
	 * Records should be sentence-segmentized before this step.
	 * 
	 * @param inputType
	 *            can be "raw-text" or "tab-separated"
	 * @param inputColumn
	 *            number of columns in tab-separated type, -1 in raw-text type
	 * @param replaceStrings
	 *            replace strings flag
	 * @param replacements
	 *            key/value pairs of strings or characters that should be replaced
	 *            if replaceStrings flag is true
	 * @param exchangeOutput
	 *            flag to write ill-formed sentences
	 * @param filterMap
	 *            contains all compiled rules (general rules, text type rules and
	 *            language rules)
	 */
	private static final long serialVersionUID = -1029331897409429578L;
	private final static Logger logger = Logger.getLogger(CleanSentence.class.getName());

	String inputType;
	int inputColumn;
	Boolean replaceStrings;
	HashMap<String, String> replacements;
	Boolean exchangeOutput;
	HashMap<Integer, SentenceFilter> filterMap;

	public CleanSentence(String _inputType, int _inputColumn, Boolean _replaceStrings,
			HashMap<String, String> _replacements,
			Boolean _exchangeOutput, HashMap<Integer, SentenceFilter> _filterMap) {
		inputType = _inputType;
		replaceStrings = _replaceStrings;
		replacements = _replacements;
		exchangeOutput = _exchangeOutput;
		filterMap = _filterMap;
		inputColumn = _inputColumn;
	}

	@Override
	public Iterator<Tuple2<Long, String>> call(Tuple2<Long, String> line) throws Exception {
		if (inputType.equals("raw-text")) {
			logger.info("Input type is raw-text.");
			return checkRawtextFile(line, replaceStrings, replacements, exchangeOutput, filterMap);
		}else if(inputType.equals("tab-separated")) {
			logger.info("Input type istab-separated.");
			return checkTabFile(line, inputColumn, replaceStrings, replacements, exchangeOutput, filterMap);
		} else {
			logger.info("Input type: " + inputType + " is invalid. No cleaning!");
			return null;
		}
}

	private Iterator<Tuple2<Long, String>> checkTabFile(Tuple2<Long, String> line, int inputColumn,
			Boolean replaceStrings, HashMap<String, String> replacements, Boolean exchangeOutput,
			HashMap<Integer, SentenceFilter> filterMap) {
		List<Tuple2<Long, String>> finalOutput = new ArrayList<Tuple2<Long, String>>();
		boolean isValid = true;
		ArrayList<String> output = new ArrayList<String>();
		String[] lineArray = line._2.split("\t");
		// replace special characters
		if (inputColumn < lineArray.length & replaceStrings & replacements.size() > 0) {
			lineArray[inputColumn] = new StringReplacements(replacements).replaceEntities(lineArray[inputColumn]);
		}
		// sequential filter checks
		isValid = true;
		Iterator<Integer> iter = filterMap.keySet().iterator();
		while (iter.hasNext()) {
			SentenceFilter filter = filterMap.get(iter.next());
			if (inputColumn >= lineArray.length || !filter.sentenceIsValid(lineArray[inputColumn])) {
				isValid = false;
				if (exchangeOutput) { // write ill-formed sentences
					String outputLine = "";
					for (int i = 0; i < lineArray.length; i++)
						outputLine += lineArray[i] + "\t";
					if (outputLine.length() > 0)
						outputLine = outputLine.substring(0, outputLine.length() - 1);
					outputLine += "\tRule: " + filter.getFilterID() + " " + filter.getFilterDescription() + "\n";
					output.add(outputLine);
				}
				break;
			}
		}
		// write output
		if (isValid && !exchangeOutput) {
			String outputLine = "";
			for (int i = 0; i < lineArray.length; i++)
				outputLine += lineArray[i] + "\t";
			if (outputLine.length() > 0)
				outputLine = outputLine.substring(0, outputLine.length() - 1);
			outputLine += "\n";
			output.add(outputLine);
		}
		for (int i = 0; i < output.size(); i++)
			finalOutput.add(new Tuple2<Long, String>(line._1, output.get(i)));

		return (finalOutput.iterator());
	}

	private Iterator<Tuple2<Long, String>> checkRawtextFile(Tuple2<Long, String> lineMetadata, Boolean replaceStrings,
			HashMap<String, String> replacements, Boolean exchangeOutput, HashMap<Integer, SentenceFilter> filterMap) {
		List<Tuple2<Long, String>> finalOutput = new ArrayList<Tuple2<Long, String>>();
		boolean isValid = true;
		ArrayList<String> output = new ArrayList<String>();
		String line = lineMetadata._2;
		// replace special characters
		if (replaceStrings & replacements.size() > 0) {
			line = new StringReplacements(replacements).replaceEntities(line);
		}
		// sequential filter checks
		isValid = true;
		Iterator<Integer> iter = filterMap.keySet().iterator();
		while (iter.hasNext()) {
			SentenceFilter filter = filterMap.get(iter.next());
			if (!filter.sentenceIsValid(line)) {
				isValid = false;
				if (exchangeOutput) // write ill-formed sentences
					output.add(line + "\tRule: " + filter.getFilterID() + " " + filter.getFilterDescription());
				break;
			}
		}
		// write well-formed sentences
		if (isValid && !exchangeOutput)
			output.add(line);
		for (int i = 0; i < output.size(); i++)
			finalOutput.add(new Tuple2<Long, String>(lineMetadata._1, output.get(i)));

		return (finalOutput.iterator());
	}

}