package SparkImp.SparkSentenceCleaner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class RuleFileParser {

	/**
	 * loads all rules from ruleFile and returns Vector with all created
	 * SentenceFilters
	 * 
	 * @param ruleFile
	 * @param type
	 * @return HashMap with all created SentenceFilters
	 */
	public static HashMap<Integer, SentenceFilter> parseRuleFile(String ruleFilePath, String type) {
		return parseRuleFile(ruleFilePath, type, false);
	}

	public static HashMap<Integer, SentenceFilter> parseRuleFile(String ruleFilePath, String type, boolean verbose) {
		HashMap<Integer, SentenceFilter> filterMap = new HashMap<Integer, SentenceFilter>();
		try {
			List<String> lines;
			lines = Files.readAllLines(Paths.get(new File(ruleFilePath).getPath()));
			SimpleSentenceFilter filter = new SimpleSentenceFilter(verbose);
			for (int i = 0; i < lines.size(); i++) {
				String line = lines.get(i);
				if (line.startsWith("#") || line.startsWith("//") || line.length() == 0) {
					continue;
				} else if (line.toUpperCase().startsWith("RULE")) {
					if (filter.filterIsValid()) {
						if (verbose)
							System.out.println("   ...added rule: " + filter.getFilterDescription());
						filterMap.put(filter.getFilterID(), filter);
						filter = new SimpleSentenceFilter(verbose);
					}
					filter.setFilterID(Integer.parseInt(line.substring(5)));
					continue;
				} else if (line.toUpperCase().startsWith("DESC:")) {
					filter.setFilterDescription(type + " - " + line.substring(5));
					continue;
				} else if (line.toUpperCase().startsWith("REGEXP:")) {
					filter.setPattern(line.substring(7));
					continue;
				} else if (line.toUpperCase().startsWith("MAXLENGTH:")) {
					filter.setMaxLength(Integer.parseInt(line.substring(10)));
					continue;
				} else if (line.toUpperCase().startsWith("MINLENGTH:")) {
					filter.setMinLength(Integer.parseInt(line.substring(10)));
					continue;
				} else if (line.toUpperCase().startsWith("REPLACE_CHARS")) {
					filter.setReplaceCharacterString(line.substring(14));
					continue;
				} else if (line.toUpperCase().startsWith("REPLACE_RATIO")) {
					filter.setReplaceRatio(Float.parseFloat(line.substring(14)));
					continue;
				} else if (line.toUpperCase().startsWith("REPLACE_COUNT")) {
					filter.setReplaceCount(Integer.parseInt(line.substring(14)));
					continue;
				} else if (line.toUpperCase().startsWith("HEX_REGEXP:")) {
					filter.setHexPattern(line.substring(11));
					continue;
				}
			}
			// last rule
			if (filter.filterIsValid()) {
				if (verbose)
					System.out.println("   ...added rule: " + filter.getFilterDescription());
				filterMap.put(filter.getFilterID(), filter);
			}
			if (verbose)
				System.out.println();

		} catch (IOException e) {
			System.out.println("!!!!!!! NO RULE FILE !!!!!!!!");
			e.printStackTrace();
		}
		return filterMap;
	}
}
