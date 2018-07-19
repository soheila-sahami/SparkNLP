package NLPpipeline;

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
	public static HashMap<Integer, SentenceFilter> parseRuleFile(List<String> lines, String type) {
		HashMap<Integer, SentenceFilter> filterMap = new HashMap<Integer, SentenceFilter>();
		SimpleSentenceFilter filter = new SimpleSentenceFilter(false);
		for (int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			if (line.toUpperCase().startsWith("RULE")) {
				if (filter.filterIsValid()) {
					filterMap.put(filter.getFilterID(), filter);
					filter = new SimpleSentenceFilter(false);
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
			filterMap.put(filter.getFilterID(), filter);
		}
		return filterMap;
	}
}
