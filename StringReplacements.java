package NLPpipeline;

import java.util.HashMap;
import java.util.Iterator;

public class StringReplacements {
	/**
	 * reads string replacement for cleaning tool
	 */
	private HashMap<String, String> replacements = new HashMap<String, String>();

	public StringReplacements(HashMap<String, String> replacementList) {
		replacements = replacementList;
	}

	public String replaceEntities(String line) {
		Iterator<String> iter = replacements.keySet().iterator();

		String key, value;
		while (iter.hasNext()) {
			key = iter.next();
			value = replacements.get(key);
			line = line.replace(key, value);
		}
		return line;
	}
}
