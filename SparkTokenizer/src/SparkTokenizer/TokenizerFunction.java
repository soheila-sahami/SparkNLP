package SparkTokenizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class TokenizerFunction implements Function<Tuple2<String, Long>, Tuple2<String, Long>> {

	List<String> abbrevList;
	List<String> explicitCut;
	List<String> fixedTokens;
	List<Tuple2<String, Integer[]>> tokenisationAction;
	String[] evilSentenceEndCharacters;
	String[] punctuationCharacters;
	String[] characters;
	boolean boolReplaceNumbers = false;

	public TokenizerFunction(List<String> _abbrevList, List<String> _explicitCut, List<String> _fixedTokens, List<Tuple2<String, Integer[]>> _tokenisationAction,
			String[] _evilSentenceEndCharacters, String[] _punctuationCharacters) {
		abbrevList = _abbrevList;
		explicitCut = _explicitCut;
		fixedTokens = _fixedTokens;
		tokenisationAction = _tokenisationAction;
		evilSentenceEndCharacters = _evilSentenceEndCharacters;
		punctuationCharacters = _punctuationCharacters;
		characters = new String[tokenisationAction.size()];
		for (int i = 0; i < tokenisationAction.size(); i++) {
			characters[i] = tokenisationAction.get(i)._1;
		}

	}

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, Long> call(Tuple2<String, Long> line) throws Exception {
		String strTokenisedWord = execute(line._1);
		return (new Tuple2<String, Long>(strTokenisedWord, line._2));
	}

	private String execute(String strLine) {
		for (String sentEndChar : evilSentenceEndCharacters) {
			for (String punct : punctuationCharacters) {
				if (strLine.endsWith(punct + sentEndChar)) {
					strLine = strLine.replace(punct + sentEndChar, " " + punct + sentEndChar);
				}
			}
		}

		ArrayList<int[]> objWhitespacePositions = getWhitespacePositions(strLine);
		Iterator<int[]> objIter = objWhitespacePositions.iterator();
		int intStart = 0;
		int intEnd = 0;
		boolean sentenceEnd = false;
		StringBuilder objBuffer = new StringBuilder();

		while (objIter.hasNext()) {
			intEnd = objIter.next()[0];
			if (intEnd - intStart > 0) {
				String strWord = strLine.substring(intStart, intEnd);

				if (!(strWord.equals("%^%") || strWord.equals("%$%") || strWord.equals("%N%") || strWord.equals("%TAB%"))) {
					if (intEnd == strLine.length()) {
						sentenceEnd = true;
					}
					String strTokenisedWord = processPrefix(strWord, sentenceEnd);
					//
					// while (!strWord.equals(strTokenisedWord)) {
					// strWord = strTokenisedWord;
					// strTokenisedWord = execute(strWord);
					// }

					strTokenisedWord = processInfix(strWord);

					// while (!strWord.equals(strTokenisedWord)) {
					// strWord = strTokenisedWord;
					// strTokenisedWord = execute(strWord);
					// }

					objBuffer.append(strTokenisedWord).append(" ");
				} else {
					objBuffer.append(strWord).append(" ");
				}
			}

			intStart = intEnd + 1;
		}
		return (objBuffer.toString().trim());
	}

	private ArrayList<int[]> getWhitespacePositions(String strCurLine) {
		ArrayList<int[]> objTokens = new ArrayList<int[]>(32);

		int intEndPos = strCurLine.length();
		for (int i = 0; i < intEndPos; i++) {
			if (Character.isWhitespace(strCurLine.charAt(i))) {
				int intPos[] = new int[1];
				intPos[0] = i;
				objTokens.add(intPos);
			}
		}

		int intPos[] = new int[1];
		intPos[0] = strCurLine.length();
		objTokens.add(intPos);
		return objTokens;
	}

	private String processPrefix(String strWord, boolean sentenceEnd) {
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
					desiredAction = tokenisationAction.get(i)._2[0];
					// use word mid action
				} else {
					desiredAction = tokenisationAction.get(i)._2[1];
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

	private String processInfix(String strLine) {

		ArrayList<int[]> objWhitespacePositions = getWhitespacePositions(strLine);
		Iterator<int[]> objIter = objWhitespacePositions.iterator();

		int intStart = 0;
		int intEnd = 0;
		StringBuilder objBuffer = new StringBuilder();

		while (objIter.hasNext()) {
			intEnd = objIter.next()[0];

			if (intEnd - intStart > 0) {
				String strWord = strLine.substring(intStart, intEnd);

				if (!(strWord.equals("%^%") || strWord.equals("%$%") || strWord.equals("%N%") || strWord.equals("%TAB%"))) {

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

	private String processSingleNumber(String strWord) {
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

	private String processSuffix(String strWord, boolean sentenceEnd) {
		if (strWord.length() == 1) {
			return processSingleNumber(strWord);// check
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
				int desiredAction = tokenisationAction.get(indesOfCharInActionList(characters[i]))._2[whatToDo];

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

	private int indesOfCharInActionList(String str) {
		for (int i = 0; i < tokenisationAction.size(); i++)
			if (str.equals(tokenisationAction.get(i)._1))
				return (i);
		return -1;
	}

	private int getCharacterLength(String strCharacter) {
		if (strCharacter == null) {
			return 1;
		}

		int length = strCharacter.length();
		if (length > 0) {
			return length;
		}

		return 0;
	}

	private String processInfix1(String strWord) {

		if (strWord.trim().length() == 1) {
			return strWord;
		}

		if (fixedTokens.contains(strWord)) {
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
				desiredAction = tokenisationAction.get(indesOfCharInActionList(characters[i]))._2[1];
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

	private String processDotAsPrefix(String strWord, Boolean sentenceEnd) {
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

		if (indesOfCharInActionList(".") != -1) {
			// get wordstart action
			int action = tokenisationAction.get(indesOfCharInActionList("."))._2[0];
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

	private boolean isSequenceOfDot(String strWord) {
		int length = strWord.length();

		for (int i = 0; i < length; i++) {
			if (strWord.charAt(i) != '.') {
				return false;
			}
		}

		return true;
	}

	private String processDotAsSuffix(String strWord, Boolean sentenceEnd) {

		if (strWord.length() == 1 || abbrevList.contains(strWord)) {
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
		if (indesOfCharInActionList(".") != -1) {
			int whatToDo = 0;
			if (!sentenceEnd) {
				// get wordend action
				whatToDo = 2;
			} else {
				whatToDo = 3;
			}

			int action = tokenisationAction.get(indesOfCharInActionList("."))._2[whatToDo];
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

	private String processInfixApostrophe(String strWord) {
		return processNumber(strWord);
	}

	private String processNumber(String strWord) {
		if (boolReplaceNumbers && isNumber(strWord)) {
			return "%N%";
		}

		return strWord;
	}

	private boolean isNumber(String strWord) {
		int length = strWord.length();

		for (int i = 0; i < length; i++) {
			if (!(Character.isDigit(strWord.charAt(i)) || (Character.getType(strWord.charAt(i)) == Character.OTHER_PUNCTUATION) || strWord.charAt(i) == '-')) {
				return false;
			}
		}
		return true;
	}

	private int detectSequenceOfDot(String strWord) {
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
}
