package SparkImp.SparkSentenceCleaner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class StringReplacements {
	private HashMap<String, String> replacements = new HashMap<String, String>();

	/**
	 * Constructor
	 */
	public StringReplacements() {
		try {
			BufferedReader reader = new BufferedReader(
					new FileReader("rules" + File.separator + "StringReplacements.list"));
			String line = "";
			String[] lineArray;
			while ((line = reader.readLine()) != null) {
				lineArray = line.split("\t");
				if (lineArray.length == 2)
					replacements.put(lineArray[0], lineArray[1]);
			}

			reader.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * replaces Strings on replaceString
	 * 
	 * @param replaceString
	 * @return String with all replacements
	 */
	public String replaceEntities(String replaceString) {
		Iterator<String> iter = replacements.keySet().iterator();

		String key, value;
		while (iter.hasNext()) {
			key = iter.next();
			value = replacements.get(key);
			replaceString = replaceString.replace(key, value);
		}

		return replaceString;
	}

	public static void main(String[] args) {
		StringReplacements sr = new StringReplacements();
		System.out.println(sr.replaceEntities(args[0]));
	}
}
