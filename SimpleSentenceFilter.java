package NLPpipeline;


import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * simple sentence filter that looks for regexp patterns or length restrictions
 * 
 * @author Thomas Eckart
 */
public class SimpleSentenceFilter implements SentenceFilter, Serializable {
	private static final long serialVersionUID = 1L;
	private Integer filterID = -1;
	private String filterDescription = "";
	private String patternString = "";
	private Pattern pattern = null;
	private int minLength = 0;
	private int maxLength = 0;
	private boolean verbose = false;
	private int hits = 0;
	private String replaceCharacterString = null;
	private float replaceRatio = 0;
	private float replaceCount = 0;
	private String hexPatternString = "";
	private Pattern hexPattern = null;

	/**
	 * constructor
	 * 
	 * @param verbose
	 */
	public SimpleSentenceFilter(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * constructor
	 * 
	 * @param filterDescription
	 * @param patternString
	 * @param minLength
	 * @param maxLength
	 * @param hexPattern
	 * @param verbose
	 */
	public SimpleSentenceFilter(String filterDescription, String patternString, int minLength, int maxLength,
			String hexPatternString, boolean verbose) {
		this.filterDescription = filterDescription;
		this.patternString = patternString;
		this.pattern = Pattern.compile(patternString);
		this.minLength = minLength;
		this.maxLength = maxLength;
		this.hexPatternString = hexPatternString;
		this.hexPattern = Pattern.compile(hexPatternString);
		this.verbose = verbose;
	}

	/**
	 * constructor
	 * 
	 * @param filterDescription
	 * @param patternString
	 * @param minLength
	 * @param maxLength
	 * @param hexPattern
	 */
	public SimpleSentenceFilter(String filterDescription, String patternString, int minLength, int maxLength,
			String hexPattern) {
		this(filterDescription, patternString, minLength, maxLength, hexPattern, false);
	}

	/**
	 * checks if this filter is valid
	 * 
	 * @return filter is valid?
	 */
	public boolean filterIsValid() {
		if (filterID != -1 && (pattern != null || hexPattern != null || minLength != 0 || maxLength != 0
				|| (replaceCharacterString != null && (replaceRatio != 0 || replaceCount != 0))))
			return true;
		else
			return false;
	}

	/**
	 * checks String sentence if it is valid regarding this filter
	 * 
	 * @param sentence
	 * @return valid?
	 */
	@Override
	public boolean sentenceIsValid(String sentence) {
		boolean regexpFailed = false;
		boolean minLengthFailed = false;
		boolean maxLengthFailed = false;
		boolean replaceRatioValid = false;
		boolean replaceCountValid = false;
		boolean hexPatternFailed = false;
		HashSet<Boolean> usedCriteria = new HashSet<Boolean>();

		// REGEXP
		if (pattern != null) {
			Matcher m = pattern.matcher(sentence);
			regexpFailed = m.find();
			usedCriteria.add(regexpFailed);
		}

		// MINLENGTH
		if (minLength != 0) {
			minLengthFailed = sentence.length() < minLength;
			usedCriteria.add(minLengthFailed);
		}

		// MAXLENGTH
		if (maxLength != 0) {
			maxLengthFailed = sentence.length() > maxLength;
			usedCriteria.add(maxLengthFailed);
		}

		// REPLACE_CHARS + REPLACE_RATIO
		if ((replaceCharacterString != null) && (replaceRatio != 0 || replaceCount != 0)) {
			String replacedSentence = sentence;
			for (int i = 0; i < replaceCharacterString.length(); i++)
				replacedSentence = replacedSentence.replace(replaceCharacterString.substring(i, i + 1), "");

			if (replaceRatio != 0) {
				replaceRatioValid = ((float) sentence.length() / (replacedSentence.length() + 1)) <= replaceRatio;
				usedCriteria.add(!replaceRatioValid);
			} else if (replaceCount != 0) {
				replaceCountValid = sentence.length() - (replacedSentence.length()) < replaceCount;
				usedCriteria.add(!replaceCountValid);
			}
		}

		// HEX_REGEXP
		if (hexPattern != null) {
			try {
				for (int i = 0; i < sentence.length(); i++) {
					String hexChar = String.format("%x",
							new BigInteger(1, String.valueOf(sentence.charAt(i)).getBytes("UTF-8")));
					// System.out.println(sentence.charAt(i) + "\t" + hexChar);
					Matcher m = hexPattern.matcher(hexChar);
					if (m.find()) {
						hexPatternFailed = true;
						break;
					}
				}
				usedCriteria.add(hexPatternFailed);
			} catch (UnsupportedEncodingException usee) {
				usee.printStackTrace();
			}
		}

		Iterator<Boolean> iter = usedCriteria.iterator();
		Boolean failed = iter.next();
		while (iter.hasNext()) {
			failed &= iter.next();
		}

		if (failed) {
			hits++;
			if (verbose)
				System.out.println("sentence \"" + sentence + "\" failed test: \"" + this.filterID + " - "
						+ this.filterDescription + "\"");
		}

		return !failed;
	}

	// getter methods
	@Override
	public int getFilterID() {
		return filterID;
	}

	@Override
	public String getFilterDescription() {
		if (filterDescription != "") {
			return this.filterDescription;
		} else {
			return "unknown";
		}
	}

	@Override
	public int getHits() {
		return hits;
	}

	@Override
	public SentenceFilter clone() {
		return new SimpleSentenceFilter(filterDescription, patternString, minLength, maxLength, hexPatternString,
				verbose);
	}

	// setter methods
	public void setFilterID(Integer filterID) {
		this.filterID = filterID;
	}

	public void setFilterDescription(String filterDescription) {
		this.filterDescription = filterDescription;
	}

	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}

	public void setMinLength(int minLength) {
		this.minLength = minLength;
	}

	public void setPattern(String patternString) {
		this.pattern = Pattern.compile(patternString);
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public void setReplaceCharacterString(String replaceCharacterString) {
		this.replaceCharacterString = replaceCharacterString;
	}

	public void setReplaceRatio(float replaceRatio) {
		this.replaceRatio = replaceRatio;
	}

	public void setReplaceCount(int replaceCount) {
		this.replaceCount = replaceCount;
	}

	public void setHexPattern(String hexPatternString) {
		this.hexPattern = Pattern.compile(hexPatternString);
	}

}
