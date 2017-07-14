package SparkTokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class CharacterBasedWordTokenizerImpl extends AbstractWordTokenizer {
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = Logger.getLogger(AbstractWordTokenizer.class);

	String strSentenceNumber = null;

	String strCharacterActionsFile = null;
	TreeMap<String, Integer[]> charActions = null;
	String[] characters = null;
	boolean boolReplaceNumbers = false;
	HashSet<String> objFixedTokens = null;

	/** Creates a new instance of DefaultWordTokenizer */
	public CharacterBasedWordTokenizerImpl() {
	}

	@Override
	public void init() {
		super.init();

		boolean readFixedTokFile = true;

		objFixedTokens = new HashSet<String>();

		// TODO: Lese UnTok
		if (fixedTokensFile == null || fixedTokensFile.equals("")) {
			LOGGER.warn("ATTENTION: Fixed tokens file not set.");
			readFixedTokFile = false;
		}

		if (!new File(fixedTokensFile).exists()) {
			LOGGER.warn("ATTENTION: Fixed tokens file not found: " + fixedTokensFile);
			readFixedTokFile = false;
		}

		if (readFixedTokFile) {
			try {
				BufferedReader objReader = new BufferedReader(new FileReader(fixedTokensFile));
				String strLine = null;

				while ((strLine = objReader.readLine()) != null) {

					if (!strLine.trim().equals("")) {
						objFixedTokens.add(strLine.trim());
					}
				}

				objReader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		boolean readCharActionFile = true;

		charActions = new TreeMap<String, Integer[]>();

		if (strCharacterActionsFile == null || strCharacterActionsFile.equals("")) {
			LOGGER.warn("ATTENTION: Character action file not set.");
			readCharActionFile = false;
		}

		if (!new File(strCharacterActionsFile).exists()) {
			LOGGER.warn("ATTENTION: Character action file not found: " + strCharacterActionsFile);
			readCharActionFile = false;
		}

		if (readCharActionFile) {
			try {
				BufferedReader objReader = new BufferedReader(new FileReader(strCharacterActionsFile));
				String strLine = null;

				// first line of document contains table headers, skip it
				boolean firstline = true;
				while ((strLine = objReader.readLine()) != null) {

					if (!strLine.trim().equals("")) {
						if (!firstline) {
							String[] actions = strLine.trim().split("\\t");
							Integer[] intActions = new Integer[4];
							for (int i = 2; i <= 5; i++) {
								if (actions[i].toLowerCase().equals("nothing")) {
									intActions[i - 2] = 0;
								} else if (actions[i].toLowerCase().equals("whitespace")) {
									intActions[i - 2] = 1;
								} else if (actions[i].toLowerCase().equals("delete")) {
									intActions[i - 2] = 2;
								} else {
									intActions[i - 2] = 3;
								}
							}
							charActions.put(actions[1], intActions);

						} else {
							firstline = false;
						}
					}
				}

				characters = new String[charActions.keySet().size()];
				Iterator<String> objKeyIterator = charActions.keySet().iterator();
				int i = 0;
				while (objKeyIterator.hasNext()) {
					characters[i] = objKeyIterator.next();
					i++;
				}

				objReader.close();
			} catch (ArrayIndexOutOfBoundsException oob) {
				LOGGER.warn("Character actions file malformatted.");
				oob.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public String execute(String strLine) {
		String[] evilSentenceEndCharacters = { "\"", "'", "“", "”", "„", "," };
		String[] punctuationCharacters = { ".", "!", "?", ";", "-", ":" };
		for (String sentEndChar : evilSentenceEndCharacters) {
			for (String punct : punctuationCharacters) {
				if (strLine.endsWith(punct + sentEndChar)) {
					strLine = strLine.replace(punct + sentEndChar, " " + punct + sentEndChar);
				}
			}
		}

		ArrayList<int[]> objWhitespacePositions = getWhitespacePositions(strLine);
		// ConfigurationContainer.println( "Satz: " + strLine );
		Iterator<int[]> objIter = objWhitespacePositions.iterator();

		int intStart = 0;
		int intEnd = 0;
		boolean sentenceEnd = false;
		StringBuilder objBuffer = new StringBuilder();

		while (objIter.hasNext()) {
			intEnd = objIter.next()[0];
			if (intEnd - intStart > 0) {
				String strWord = strLine.substring(intStart, intEnd);

				if (!(strWord.equals("%^%") || strWord.equals("%$%") || strWord.equals("%N%")
						|| strWord.equals("%TAB%"))) {

					if (intEnd == strLine.length()) {
						sentenceEnd = true;
					}
					String strTokenisedWord = processPrefix(strWord, sentenceEnd);

					while (!strWord.equals(strTokenisedWord)) {
						strWord = strTokenisedWord;
						strTokenisedWord = execute(strWord);
					}

					strTokenisedWord = processInfix(strWord);

					while (!strWord.equals(strTokenisedWord)) {
						strWord = strTokenisedWord;
						strTokenisedWord = execute(strWord);
					}

					objBuffer.append(strTokenisedWord).append(" ");
				} else {
					objBuffer.append(strWord).append(" ");
				}
			}

			intStart = intEnd + 1;
		}
		return objBuffer.toString().trim();
	}

	protected String processPrefix(String strWord, boolean sentenceEnd) {
		if (strWord == null) {
			return "";
		}
		if (strWord.length() == 0) {
			return strWord;
		}
		if (strWord.length() == 1) {
			return processSingleNumber(strWord);
		}

		StringBuilder objBuffer = new StringBuilder();
		int intOffSet = 0;
		int characterCount = characters.length;
		int desiredAction = 0;
		// for every character to tokenize by
		for (int i = 0; i < characterCount; i++) {

			if (strWord.startsWith(characters[i], intOffSet)) {
				int intLengthOfCharacter = getCharacterLength(characters[i]);

				// use word start action
				if (intOffSet == 0) {
					desiredAction = charActions.get(characters[i])[0];
					// use word mid action
				} else {
					desiredAction = charActions.get(characters[i])[1];
				}
				if (desiredAction == 1) {
					// whitespace
					objBuffer.append(strWord.substring(intOffSet, intOffSet + intLengthOfCharacter));
					objBuffer.append(" ");
				} else if (desiredAction == 0 || desiredAction == 3) {
					// do nothing
					objBuffer.append(strWord.substring(intOffSet, intOffSet + intLengthOfCharacter));
				} else if (desiredAction == 2) {
					// delete the character
				} else {
					// this should not happen. Append anyway to not lose
					// something.
					objBuffer.append(strWord.substring(intOffSet, intOffSet + intLengthOfCharacter));
				}

				// next position of string
				intOffSet += intLengthOfCharacter;
				// do it again
				i = 0;
			}
		}
		objBuffer.append(processSuffix(strWord.substring(intOffSet), sentenceEnd));
		return objBuffer.toString().trim();
	}

	protected String processSuffix(String strWord, boolean sentenceEnd) {
		if (strWord.length() == 1) {
			return processSingleNumber(strWord);
		}
		StringBuilder objBuffer = new StringBuilder();
		String strResult = "";
		int characterCount = characters.length;
		int whatToDo = 0;

		Boolean sentenceEndTMP = sentenceEnd;
		String strTMPWord = new String(strWord);

		for (int i = 0; i < characterCount; i++) {

			if (strTMPWord.endsWith(characters[i])) {

				int intLengthOfCharacter = getCharacterLength(characters[i]);

				if (!sentenceEndTMP) {
					whatToDo = 2;
				} else {
					whatToDo = 3;
					sentenceEndTMP = false;
				}

				int desiredAction = charActions.get(characters[i])[whatToDo];

				if (desiredAction == 1) {
					// whitespace before char
					strResult = " " + strTMPWord.substring(strTMPWord.length() - intLengthOfCharacter) + strResult;
					strTMPWord = strTMPWord.substring(0, strTMPWord.length() - intLengthOfCharacter);
				} else if (desiredAction == 0 || desiredAction == 3) {
					// do nothing
					strResult = strTMPWord.substring(strTMPWord.length() - intLengthOfCharacter) + strResult;
					strTMPWord = strTMPWord.substring(0, strTMPWord.length() - intLengthOfCharacter);
				} else if (desiredAction == 2) {
					// delete the character
					strTMPWord = strTMPWord.substring(0, strTMPWord.length() - intLengthOfCharacter);
				} else {
					// this should not happen. Append anyway to not lose
					// something.
					strResult = strTMPWord.substring(strTMPWord.length() - intLengthOfCharacter) + strResult;
					strTMPWord = strTMPWord.substring(0, strTMPWord.length() - intLengthOfCharacter);
				}
			}
		}
		// by now, we should not have any dots in it anymore. Why do we do this?
		objBuffer.append(processDotAsPrefix(strTMPWord, sentenceEnd));
		objBuffer.append(strResult);

		return objBuffer.toString().trim();
	}

	@Override
	protected int getCharacterLength(String strCharacter) {

		if (strCharacter == null) {
			return 1;
		}

		int length = strCharacter.length();
		if (length > 0) {
			return length;
		}

		return 0;
	}

	protected boolean isSequenceOfDot(String strWord) {
		int length = strWord.length();

		for (int i = 0; i < length; i++) {
			if (strWord.charAt(i) != '.') {
				return false;
			}
		}

		return true;
	}

	protected String processDotAsPrefix(String strWord, Boolean sentenceEnd) {
		if (strWord.length() == 1 || isSequenceOfDot(strWord)) {
			return processSingleNumber(strWord);
		}

		if (!strWord.startsWith(".")) {
			StringBuilder objBuffer = new StringBuilder();
			objBuffer.append(processDotAsSuffix(strWord, sentenceEnd));
			// this is trimmed in the next line?!
			objBuffer.append(" ");
			return objBuffer.toString().trim();
		}

		StringBuilder objBuffer = new StringBuilder();
		if (charActions.containsKey(".")) {
			// get wordstart action
			int action = charActions.get(".")[0];
			if (action == 1) {
				// whitespace
				objBuffer.append(".");
				objBuffer.append(" ");
			} else if (action == 0 || action == 3) {
				// do nothing
				objBuffer.append(".");
			} else if (action == 2) {
				// delete the character
			} else {
				// this should not happen. Append anyway to not lose something.
				objBuffer.append(".");
			}
			// fallback to simple append
		} else {
			objBuffer.append(".");
		}
		objBuffer.append(processDotAsSuffix(strWord.substring(1), sentenceEnd));

		return objBuffer.toString().trim();
	}

	protected int detectSequenceOfDot(String strWord) {
		int length = strWord.length();
		int returnLength = 0;
		for (int i = length - 1; i >= 0; i--) {
			if (strWord.charAt(i) != '.') {
				return returnLength;
			}
			returnLength++;
		}
		return 0;
	}

	protected String processDotAsSuffix(String strWord, Boolean sentenceEnd) {

		if (strWord.length() == 1 || objAbbrevList.contains(strWord)) {
			return processSingleNumber(strWord);
		}

		// word not ends with .
		if (!strWord.endsWith(".")) {
			StringBuilder objBuffer = new StringBuilder();
			objBuffer.append(processInfixApostrophe(strWord));
			objBuffer.append(" ");
			return objBuffer.toString().trim();
		}
		// word ends with .

		int intDotSeqSize = detectSequenceOfDot(strWord);

		if (intDotSeqSize > 1) {
			StringBuilder objBuffer = new StringBuilder();
			objBuffer.append(processInfixApostrophe(strWord.substring(0, strWord.length() - intDotSeqSize)));
			objBuffer.append(" ");

			for (int i = 0; i < intDotSeqSize; i++) {
				objBuffer.append(".");
			}

			return objBuffer.toString().trim();
		}

		StringBuilder objBuffer = new StringBuilder();
		objBuffer.append(processInfixApostrophe(strWord.substring(0, strWord.length() - 1)));
		if (charActions.containsKey(".")) {
			int whatToDo = 0;
			if (!sentenceEnd) {
				// get wordend action
				whatToDo = 2;
			} else {
				whatToDo = 3;
			}
			int action = charActions.get(".")[whatToDo];
			if (action == 1) {
				// whitespace
				objBuffer.append(".");
				objBuffer.append(" ");
			} else if (action == 0 || action == 3) {
				// do nothing
				objBuffer.append(".");
			} else if (action == 2) {
				// delete the character
			} else {
				// this should not happen. Append anyway to not lose something.
				objBuffer.append(".");
			}
			// fallback to simple append
		} else {
			objBuffer.append(" .");
		}
		return objBuffer.toString();
	}

	protected boolean isNumber(String strWord) {
		int length = strWord.length();

		for (int i = 0; i < length; i++) {
			if (!(Character.isDigit(strWord.charAt(i))
					|| (Character.getType(strWord.charAt(i)) == Character.OTHER_PUNCTUATION)
					|| strWord.charAt(i) == '-')) {
				return false;
			}
		}

		return true;
	}

	protected String processSingleNumber(String strWord) {
		if (strWord == null) {
			return null;
		}
		if (strWord.length() == 0) {
			return strWord;
		}
		if (boolReplaceNumbers && Character.isDigit(strWord.charAt(0))) {
			return "%N%";
		}

		return strWord;
	}

	protected String processInfixApostrophe(String strWord) {
		return processNumber(strWord);
	}

	protected String processInfix(String strLine) {

		ArrayList<int[]> objWhitespacePositions = getWhitespacePositions(strLine);
		Iterator<int[]> objIter = objWhitespacePositions.iterator();

		int intStart = 0;
		int intEnd = 0;
		StringBuilder objBuffer = new StringBuilder();

		while (objIter.hasNext()) {
			intEnd = objIter.next()[0];

			if (intEnd - intStart > 0) {
				String strWord = strLine.substring(intStart, intEnd);

				if (!(strWord.equals("%^%") || strWord.equals("%$%") || strWord.equals("%N%")
						|| strWord.equals("%TAB%"))) {

					String strTokenisedWord = processInfix1(strWord);

					while (!strWord.equals(strTokenisedWord)) {
						strWord = strTokenisedWord;
						strTokenisedWord = processInfix(strWord);
					}

					objBuffer.append(strTokenisedWord).append(" ");
				} else {
					objBuffer.append(strWord).append(" ");
				}
			}

			intStart = intEnd + 1;
		}

		return objBuffer.toString().trim();

	}

	protected String processInfix1(String strWord) {

		if (strWord.trim().length() == 1) {
			return strWord;
		}

		if (this.objFixedTokens.contains(strWord)) {
			return strWord;
		}

		int characterCount = characters.length;
		int desiredAction = 0;
		String strTMPWord = strWord;
		// for every character to tokenize by
		StringBuilder objBuffer = new StringBuilder();
		for (int i = 0; i < characterCount; i++) {

			int charPos = strTMPWord.indexOf(characters[i]);
			if (charPos >= 0) {
				if (charPos == strTMPWord.length() - 1 || charPos == 0) {
					if (objBuffer.indexOf(characters[i]) < 0) {
						// wordEnd already been handled by processSuffix
						continue;
					} else {
						// objBuffer contains first part of word with multiple
						// instances of a found character
						objBuffer.append(strWord);
						return objBuffer.toString();
					}
				}
				int intLengthOfCharacter = getCharacterLength(characters[i]);
				// use word mid action
				desiredAction = charActions.get(characters[i])[1];

				if (desiredAction == 1) {
					strTMPWord = strTMPWord.substring(charPos + intLengthOfCharacter);
					objBuffer.append(strWord.substring(0, charPos) + " " + characters[i]);
					objBuffer.append(" ");
				} else if (desiredAction == 0 || desiredAction == 3) {
					// do nothing
					strTMPWord = strTMPWord.substring(charPos + intLengthOfCharacter);
					objBuffer.append(strWord.substring(0, charPos + intLengthOfCharacter));
				} else if (desiredAction == 2) {
					// delete the character
					strTMPWord = strTMPWord.substring(charPos + intLengthOfCharacter);
					objBuffer.append(strWord.substring(0, charPos));
				} else {
					// this should not happen. Append anyway to not lose
					// something.
					strTMPWord = strTMPWord.substring(charPos + intLengthOfCharacter);
					objBuffer.append(strWord.substring(0, charPos + intLengthOfCharacter));
				}

				if (strTMPWord.indexOf(characters[i]) >= 0) {
					i--;
					strWord = strTMPWord;
					continue;
				} else {
					objBuffer.append(strTMPWord);
					strTMPWord = objBuffer.toString();
					strWord = strTMPWord;
					objBuffer.delete(0, objBuffer.length());
				}

			}

		}

		return strWord;
	}

	protected String processNumber(String strWord) {
		if (boolReplaceNumbers && isNumber(strWord)) {
			return "%N%";
		}

		return strWord;
	}

	public void setFixedTokensFile(String f) {
		fixedTokensFile = f;
	}

	public void setCharacterActionsFile(String f) {
		strCharacterActionsFile = f;
	}
}
