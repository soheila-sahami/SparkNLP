package NLPpipeline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class SegmentizeDocument implements PairFlatMapFunction<Tuple2<Long, String>, Long, String> {
	/***
	 * apply the sentence segmetizer function on all records of recored set
	 * (documents are splited on \n ).
	 * 
	 * @param abbreviations
	 *            List list of abbreviations
	 * @param postList
	 *            list of strings which can be after the punctuation characters
	 * @param compiledPreRulesList
	 *            rules for the conditions which can be before the punctuation
	 *            characters
	 * @param compiledPostRulesList
	 *            rules for the conditions which can be after the punctuation
	 *            characters
	 * @param boundaries
	 *            list of known sentence boundary characters
	 * @param encoding
	 *            text encoding
	 * @param longestAbbrevation
	 *            maximum length of abbreviations
	 * @param offset
	 *            in combination of offset, text id and index of splited text, an
	 *            ordered unique id is created for each sentence
	 */

	private static final long serialVersionUID = 1L;
	List<String> abbrevations;
	List<Tuple2<Pattern, Boolean>> compiledPostRulesList;
	List<Tuple2<Pattern, Boolean>> compiledPreRulesList;
	List<String> postList;
	List<String> boundaries;
	String encoding;
	int longestAbbrevation;
	int postBoundaryWindowSize = 20;
	int offset;

	public SegmentizeDocument(List<String> reversedAbbrevation, List<String> boundariesList,
			List<Tuple2<Pattern, Boolean>> compiledPostRuleSet, List<Tuple2<Pattern, Boolean>> compiledPreRuleSet,
			List<String> postListBoundaris, int longestAbbrevationLength, String encoding1, int offset1) {
		abbrevations = reversedAbbrevation;
		boundaries = boundariesList;
		compiledPostRulesList = compiledPostRuleSet;
		compiledPreRulesList = compiledPreRuleSet;
		postList = postListBoundaris;
		encoding = encoding1;
		longestAbbrevation = longestAbbrevationLength;
		offset = offset1;
	}

	@Override
	public Iterator<Tuple2<Long, String>> call(Tuple2<Long, String> doc) throws Exception {
		Long sourceID = doc._1;
		String text = doc._2;
		int counter = 1;
		List<Tuple2<Long, String>> sentences = new ArrayList<Tuple2<Long, String>>();

		String[] lines = text.split("\n");
		for (int i = 0; i < lines.length; i++) {
			int posInWindow = 0;
			boolean completed = false;
			String line = lines[i];
			while (!completed) {
				int[] boundaryCandidate = findNextSentenceBoundary(line, posInWindow);
				if (boundaryCandidate[0] != -1) {
					if (isSentenceBoundary(line, boundaryCandidate)) {
						sentences.add(new Tuple2<Long, String>(sourceID * offset + counter,
								line.substring(0, boundaryCandidate[0] + boundaryCandidate[1])));
						counter++;
						if (boundaryCandidate[0] + boundaryCandidate[1] + 1 < line.length()) {
							line = line.substring(boundaryCandidate[0] + boundaryCandidate[1] + 1, line.length());
							posInWindow = 0;
						} else {
							completed = true;
						}
					} else {
						posInWindow = boundaryCandidate[0] + boundaryCandidate[1];
					}
				} else if (line.length() > 0) {
					sentences.add(new Tuple2<Long, String>(sourceID * offset + counter, line));
					counter++;
					completed = true;

				} else if (line.length() == 0) {
					completed = true;
				}
			}
		}
		return sentences.iterator();
	}

	private int[] findNextSentenceBoundary(String textWindow, int posInWindow) {
		int[] closestIndex = new int[] { -1, -1 };
		int foundIndex = -1;

		if (boundaries.isEmpty()) {
			return closestIndex;
		}
		for (String sentenceBoundary : boundaries) {
			foundIndex = textWindow.indexOf(sentenceBoundary, posInWindow);
			if (foundIndex != -1) {
				if (closestIndex[0] == -1 || foundIndex < closestIndex[0]) {
					closestIndex = new int[] { foundIndex, sentenceBoundary.length() };
					textWindow = textWindow.substring(0, foundIndex + sentenceBoundary.length());
				}
			}
		}
		return closestIndex;
	}

	private boolean isSentenceBoundary(final String textWindow, final int[] boundaryCandidate) {
		if (checkPreList(textWindow, boundaryCandidate))
			return false;
		if (!checkPreRules(textWindow, boundaryCandidate))
			return false;
		if (checkPostList(textWindow, boundaryCandidate))
			return false;
		if (!checkPostRules(textWindow, boundaryCandidate))
			return false;
		return true;
	}

	private String fetchNextTokenAfterPos(final String textWindow, final int[] boundaryCandidate) {
		int afterBoundaryPos = boundaryCandidate[0] + boundaryCandidate[1];
		int pos = textWindow.indexOf(" ", afterBoundaryPos);
		if (pos == afterBoundaryPos) {
			afterBoundaryPos++;
			pos = textWindow.indexOf(" ", afterBoundaryPos);
		}
		if (pos == -1) {
			pos = textWindow.length();
		}
		final String postBoundaryToken = textWindow.substring(afterBoundaryPos, pos);
		for (final String boundary : boundaries) {
			if (postBoundaryToken.endsWith(boundary)) {
				return postBoundaryToken.substring(0, postBoundaryToken.length() - boundary.length());
			}
		}
		return postBoundaryToken;
	}

	private boolean checkPostRules(String textWindow, int[] boundaryCandidate) {
		final String postBoundaryToken = fetchNextTokenAfterPos(textWindow, boundaryCandidate);
		if (postBoundaryToken.equals(" ")) {
			return true;
		}
		for (Tuple2<Pattern, Boolean> rule : compiledPostRulesList) {
			final Matcher matcher = rule._1().matcher(postBoundaryToken);
			if (matcher.matches()) {
				return rule._2();
			}
		}
		return true;
	}

	private boolean checkPostList(String textWindow, int[] boundaryCandidate) {
		final String postBoundaryToken = fetchNextTokenAfterPos(textWindow, boundaryCandidate);
		if (postBoundaryToken.equals(" ")) {
			return false;
		}
		if (postList.contains(postBoundaryToken)) {
			return true;
		}
		return false;
	}

	private boolean checkPreRules(String textWindow, int[] boundaryCandidate) {
		String candidateAbbrev = fetchPreviousTokenBeforePos(textWindow, boundaryCandidate);
		for (Tuple2<Pattern, Boolean> rule : compiledPreRulesList) {
			Matcher matcher = rule._1().matcher(candidateAbbrev);
			if (matcher.matches()) {
				return rule._2();
			}
		}
		return true;
	}

	private String fetchPreviousTokenBeforePos(final String textWindow, final int[] boundaryCandidate) {
		int firstPos = boundaryCandidate[0] - longestAbbrevation > 0 ? boundaryCandidate[0] - longestAbbrevation : 0;
		String candidateText = textWindow.substring(firstPos, boundaryCandidate[0] + boundaryCandidate[1]);
		// space & non-breaking space
		int indexOfLastSpace = Math.max(candidateText.lastIndexOf(" "), candidateText.lastIndexOf((char) (160)));
		String candidateAbbrevCandidate = indexOfLastSpace != -1 ? candidateText.substring(indexOfLastSpace + 1)
				: candidateText;
		return candidateAbbrevCandidate.trim();
	}

	private boolean checkPreList(String textWindow, int[] boundaryCandidate) {
		String candidateAbbrev = fetchPreviousTokenBeforePos(textWindow, boundaryCandidate);
		if (abbrevations.contains(candidateAbbrev)) {
			return true;
		} else {
			return false;
		}
	}
}
