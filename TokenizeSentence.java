package NLPpipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class TokenizeSentence implements PairFlatMapFunction<Tuple2<Long, String>, Long, String> {
	/***
	 * apply the tokenizer function on all records of recored set (sentences).
	 * Records should be sentence-segmentized before this step.
	 * 
	 * @param artificialSpace
	 *            to distinguish between the original spaces in the text and those
	 *            which are added by tokenizer
	 * @param abbreviation
	 *            List list of abbreviations
	 * @param postfixList
	 *            list of strings which can be after the punctuation characters
	 * @param prefixList
	 *            list of strings which can be before the punctuation characters
	 * @param preRulesSet
	 *            rules for the conditions which can be before the punctuation
	 *            characters
	 * @param postRulesSet
	 *            rules for the conditions which can be after the punctuation
	 *            characters
	 * @param fixedTokens
	 *            list of known tokens which contain punctuation
	 * @param tokenisationAction
	 *            supposed actions for each punctuation character based on its
	 *            location in the string
	 */
	private static final long serialVersionUID = 7908178017913147676L;

	private final static Logger logger = Logger.getLogger(TokenizeSentence.class.getName());

	String artificialSpace;
	List<String> abbreviationList;
	List<String> postfixList;
	List<String> prefixList;
	List<Tuple2<Pattern, Boolean>> preRulesSet;
	List<Tuple2<Pattern, Boolean>> postRulesSet;
	List<String> fixedTokens;
	List<Tuple2<String, Integer[]>> tokenisationAction;
	List<String> characters;

	public TokenizeSentence(List<String> _abbrevList, List<String> _fixedTokens, List<String> _prefilxList,
			List<String> _postfilxList, List<Tuple2<Pattern, Boolean>> _preRulesSet,
			List<Tuple2<Pattern, Boolean>> _postRulesSet, List<Tuple2<String, Integer[]>> _tokenisationAction,
			String _artificialSpace) {
		abbreviationList = _abbrevList;
		fixedTokens = _fixedTokens;
		prefixList = _prefilxList;
		postfixList = _postfilxList;
		preRulesSet = _preRulesSet;
		postRulesSet = _postRulesSet;
		tokenisationAction = _tokenisationAction;
		artificialSpace = _artificialSpace;
		characters = new ArrayList<String>();
		for (int i = 0; i < tokenisationAction.size(); i++) {
			characters.add(tokenisationAction.get(i)._1);
		}
	}

	@Override
	public Iterator<Tuple2<Long, String>> call(Tuple2<Long, String> line) throws Exception {
		String[] tempTokens = line._2().split(" ");

		List<String> tokens = new ArrayList<String>();
		boolean endOfSent = false;
		for (int i = 0; i < tempTokens.length; i++) {
			if (tempTokens[i].length() > 0)
				if (i == tempTokens.length - 1) {
					endOfSent = true;
				} else {
					endOfSent = false;
				}
			tokens.add(execute(tempTokens[i], endOfSent));
		}
		List<Tuple2<Long, String>> tmp = new ArrayList<Tuple2<Long, String>>();
		tmp.add(new Tuple2<Long, String>(line._1(), String.join(" ", tokens)));
		return (tmp.iterator());
	}

	private String execute(String strWord, boolean endOfSent) {
		if (strWord.length() == 1) {
			return strWord;
		}
		List<Integer> posOfPunctuation = posOfPaunctuationCharacters(strWord);

		if (fixedTokens.contains(strWord) || posOfPunctuation.size() == 0) {
			return strWord;
		}
		posOfPunctuation = checkForAbbreviation(strWord, posOfPunctuation);
		if (posOfPunctuation.size() > 0) {
			Tuple2<String, List<Integer>> temp = CheckForCharacterAction(strWord, posOfPunctuation, endOfSent);
			strWord = temp._1;
			posOfPunctuation = temp._2;
		}
		if (posOfPunctuation.size() > 0) {
			posOfPunctuation = checkForPostfix(strWord, posOfPunctuation);
		}
		if (posOfPunctuation.size() > 0) {
			posOfPunctuation = CheckForPrefix(strWord, posOfPunctuation);
		}
		if (posOfPunctuation.size() > 0) {
			posOfPunctuation = checkPreRules(strWord, posOfPunctuation);
		}
		if (posOfPunctuation.size() > 0) {
			posOfPunctuation = checkPostRules(strWord, posOfPunctuation);
		}
		while (posOfPunctuation.size() > 0) {
			boolean twoArtificialSpaceAdded = false;
			int pos = posOfPunctuation.get(0);
			if (pos == 0) {// first character
				strWord = strWord.charAt(0) + artificialSpace + strWord.substring(pos + 1);
			} else if (pos == strWord.length() - 1) {// last character
				strWord = strWord.substring(0, pos) + artificialSpace + strWord.charAt(pos);
			} else if (posOfPunctuation.size() > 1 && posOfPunctuation.get(0) + 1 == posOfPunctuation.get(1)) {
				strWord = strWord.substring(0, pos) + artificialSpace + strWord.substring(pos);
			} else {
				strWord = strWord.substring(0, pos) + artificialSpace + strWord.charAt(pos) + artificialSpace
						+ strWord.substring(pos + 1);
				twoArtificialSpaceAdded = true;
			}
			posOfPunctuation.remove(0);
			if (twoArtificialSpaceAdded) {
				for (int id = 0; id < posOfPunctuation.size(); id++)
					posOfPunctuation.set(id, posOfPunctuation.get(id) + (2 * artificialSpace.length()));
				twoArtificialSpaceAdded = false;
			} else {
				for (int id = 0; id < posOfPunctuation.size(); id++)
					posOfPunctuation.set(id, posOfPunctuation.get(id) + artificialSpace.length());
			}
		}
		return strWord;
	}

	private Tuple2<String, List<Integer>> CheckForCharacterAction(String strWord, List<Integer> posOfPunctuation,
			boolean endOfSent) {
		Tuple2<String, List<Integer>> temp = new Tuple2<String, List<Integer>>(strWord, posOfPunctuation);
		for (int i = 0; i < tokenisationAction.size(); i++) {
			if (posOfPunctuation.size() > 0) {
				int j = 0;
				while (j < posOfPunctuation.size()) {
					int end = (posOfPunctuation.get(j) + tokenisationAction.get(i)._1().length() > strWord.length()
							? strWord.length()
							: posOfPunctuation.get(j) + tokenisationAction.get(i)._1().length());
					if (strWord.substring(posOfPunctuation.get(j), end).equals(tokenisationAction.get(i)._1())) {
						logger.debug("Action is applied for character: " + posOfPunctuation.get(j));
						temp = applyDesiredAction(strWord, posOfPunctuation, j, tokenisationAction.get(i)._2,
								endOfSent);
						strWord = temp._1();
						posOfPunctuation = temp._2();
					}
					j++;
				}
			} else {
				break;
			}
		}
		return temp;
	}

	private Tuple2<String, List<Integer>> applyDesiredAction(String strWord, List<Integer> posOfPunctuation,
			int indexInPosOfPunctuation, Integer[] actionsList, boolean endOfSent) {
		int desiredAction = 3;
		if (posOfPunctuation.get(indexInPosOfPunctuation) == 0) {// word start action
			desiredAction = actionsList[0];
		} else if (posOfPunctuation.get(indexInPosOfPunctuation) < strWord.length() - 1) {// word middle action
			desiredAction = actionsList[1];
		} else if (posOfPunctuation.get(indexInPosOfPunctuation) == strWord.length() - 1) {// word end action
			if (endOfSent) { // end of sentence
				desiredAction = actionsList[3];
			} else {
				desiredAction = actionsList[2];
			}
		}
		switch (desiredAction) {
		case 0: // nothing
			posOfPunctuation.remove(indexInPosOfPunctuation);
			break;
		case 1: // whitespace
			if (posOfPunctuation.get(indexInPosOfPunctuation) == 0) {
				strWord = strWord.substring(0, 1) + artificialSpace + strWord.substring(1);
			} else {
				strWord = strWord.substring(0, posOfPunctuation.get(indexInPosOfPunctuation)) + artificialSpace
						+ strWord.substring(posOfPunctuation.get(indexInPosOfPunctuation));
			}
			posOfPunctuation.remove(indexInPosOfPunctuation);
			for (int id = indexInPosOfPunctuation; id < posOfPunctuation.size(); id++)
				posOfPunctuation.set(id, posOfPunctuation.get(id) + artificialSpace.length());
			break;
		case 2: // delete
			strWord = strWord.substring(0, posOfPunctuation.get(indexInPosOfPunctuation))
					+ strWord.substring(posOfPunctuation.get(indexInPosOfPunctuation) + 1);
			posOfPunctuation.remove(indexInPosOfPunctuation);
			for (int id = indexInPosOfPunctuation; id < posOfPunctuation.size(); id++)
				posOfPunctuation.set(id, posOfPunctuation.get(id) + artificialSpace.length());
			break;
		default:
			break;

		}
		return new Tuple2<String, List<Integer>>(strWord, posOfPunctuation);
	}

	private List<Integer> checkForAbbreviation(String strWord, List<Integer> posOfPunctuation) {
		for (int j = 0; j < posOfPunctuation.size(); j++) {
			if (String.valueOf(strWord.charAt(posOfPunctuation.get(j))).equals(".")
					&& strWord.length() > posOfPunctuation.get(j)) {
				String str = strWord.substring(0, posOfPunctuation.get(j) + 1);
				Optional<String> queryResult = abbreviationList.stream().filter(value -> value.equalsIgnoreCase(str))
						.findFirst();
				if (queryResult.isPresent()) {
					posOfPunctuation.remove(j);
				}
			}
		}
		return posOfPunctuation;
	}

	private List<Integer> CheckForPrefix(String strWord, List<Integer> posOfPunctuation) {
		for (int j = 0; j < posOfPunctuation.size(); j++) {
			if (String.valueOf(strWord.charAt(posOfPunctuation.get(j))).equals(".")
					&& strWord.length() > posOfPunctuation.get(j)) {
				String str = strWord.substring(0, posOfPunctuation.get(j) + 1);
				Optional<String> queryResult = prefixList.stream().filter(value -> value.equalsIgnoreCase(str))
						.findFirst();
				if (queryResult.isPresent()) {
					posOfPunctuation.remove(j);
				}
			}
		}
		return posOfPunctuation;
	}

	private List<Integer> checkForPostfix(String strWord, List<Integer> posOfPunctuation) {
		for (int i = 0; i < postfixList.size(); i++) {
			for (int j = 0; j < posOfPunctuation.size(); j++) {
				if (strWord.substring(posOfPunctuation.get(j)).toLowerCase()
						.indexOf(postfixList.get(i).toLowerCase()) == 1) {
					posOfPunctuation.remove(j);
					logger.debug("Postfix " + postfixList.get(i) + " is founded!");
				}
			}
		}
		return posOfPunctuation;
	}

	private List<Integer> checkPostRules(String strWord, List<Integer> posOfPunctuation) {
		for (Tuple2<Pattern, Boolean> rule : postRulesSet) {
			if (posOfPunctuation.size() > 0) {
				final Matcher matcher = rule._1().matcher(strWord);
				if (matcher.matches()) {
					if (!rule._2()) { // is not a boundary
						int start = matcher.start();
						int end = matcher.end() - 1;
						List<Integer> beRemoved = new ArrayList<Integer>();
						for (int i = 0; i < posOfPunctuation.size(); i++) {
							if (posOfPunctuation.get(i) >= start && posOfPunctuation.get(i) <= end) {
								beRemoved.add(posOfPunctuation.get(i));
							}
						}

						for (Integer r : beRemoved) {
							posOfPunctuation.remove(r);
						}
					}
				}
			}
		}
		return posOfPunctuation;
	}

	private List<Integer> checkPreRules(String strWord, List<Integer> posOfPunctuation) {
		for (Tuple2<Pattern, Boolean> rule : preRulesSet) {
			if (posOfPunctuation.size() > 0) {
				final Matcher matcher = rule._1().matcher(strWord);
				if (matcher.matches()) {
					if (!rule._2()) { // is not a boundary
						int start = matcher.start();
						int end = matcher.end() - 1;
						List<Integer> beRemoved = new ArrayList<Integer>();
						for (int i = 0; i < posOfPunctuation.size(); i++) {
							if (posOfPunctuation.get(i) >= start && posOfPunctuation.get(i) <= end) {
								beRemoved.add(posOfPunctuation.get(i));
							}
						}

						for (Integer r : beRemoved) {
							posOfPunctuation.remove(r);
						}
					}
				}
			}
		}
		return posOfPunctuation;
	}

	private List<Integer> posOfPaunctuationCharacters(String strWord) {
		List<Integer> posOfPunctuation = new ArrayList<Integer>();
		for (int i = 0; i < characters.size(); i++) {
			int index = strWord.indexOf(characters.get(i));
			while (index >= 0) {
				posOfPunctuation.add(index);
				index = strWord.indexOf(characters.get(i), index + 1);
			}
		}
		Collections.sort(posOfPunctuation);
		return posOfPunctuation;
	}
}
