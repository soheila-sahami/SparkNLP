package sentenceSegmentizer;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class doSegmentation implements FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
	private static final long serialVersionUID = 1L;
	List<String> reversedAbbrevations;
	List<Tuple2<Pattern, Boolean>> compiledPostRulesList;
	List<Tuple2<Pattern, Boolean>> compiledPreRulesList;
	List<String> postList;

	List<String> boundaries;
	String encoding;
	int longestAbbrevation;
	int postBoundaryWindowSize = 20;

	public doSegmentation(List<String> reversedAbbrevation, List<String> boundariesList, List<Tuple2<Pattern, Boolean>> compiledPostRuleSet,
			List<Tuple2<Pattern, Boolean>> compiledPreRuleSet, List<String> postListBoundaris, int longestAbbrevationLength, String encoding1) {
		reversedAbbrevations = reversedAbbrevation;
		boundaries = boundariesList;
		compiledPostRulesList = compiledPostRuleSet;
		compiledPreRulesList = compiledPreRuleSet;
		postList = postListBoundaris;
		encoding = encoding1;
		longestAbbrevation = longestAbbrevationLength;
		System.out.println("reversedAbbrevation: " + reversedAbbrevation.size() + "\nboundariesList: " + boundariesList.size() + "\ncompiledPostRuleSet: "
				+ compiledPostRuleSet.size() + "\ncompiledPreRuleSet: " + compiledPreRuleSet.size() + "\npostListBoundaris:  " + postListBoundaris.size()
				+ "\nlongestAbbrevationLength : " + longestAbbrevationLength);
		System.out.println(new Date() + "Starting sentence segmentizer: ");
	}

	@Override
	public Iterator call(Tuple2<String, Long> line) throws Exception {
		List<Tuple2<String, Long>> sentences = new ArrayList<Tuple2<String, Long>>();
		int posInWindow = 0;
		boolean completed = false;
		String text = line._1;
		while (!completed) {
			int[] boundaryCandidate = findNextSentenceBoundary(text, posInWindow);
			if (boundaryCandidate[0] != -1) {
				if (isSentenceBoundary(text, boundaryCandidate)) {
					sentences.add(new Tuple2<String, Long>(text.substring(0, boundaryCandidate[0] + boundaryCandidate[1]), line._2()));
					if (boundaryCandidate[0] + boundaryCandidate[1] + 1 < text.length()) {
						text = text.substring(boundaryCandidate[0] + boundaryCandidate[1] + 1, text.length());
						posInWindow = 0;
					} else {
						completed = true;
					}
				} else {
					posInWindow = boundaryCandidate[0] + boundaryCandidate[1];
				}
			} else if (text.length() > 0) {
				sentences.add(new Tuple2<String, Long>(text, line._2));
				completed = true;
			} else if (text.length() == 0) {
				completed = true;
			}
		}
		return sentences.iterator();
	}

	private int[] findNextSentenceBoundary(String textWindow, int posInWindow) {
		int[] closestIndex = new int[] { -1, -1 };
		int foundIndex = -1;

		if (!boundaries.isEmpty()) {
		} else {
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
		if (checkPreList(textWindow, boundaryCandidate)) {
			return false;
		} else {
			if (checkPreRules(textWindow, boundaryCandidate)) {
				return false;
			} else {
				if (checkPostList(textWindow, boundaryCandidate)) {
					return false;
				} else {
					if (checkPostRules(textWindow, boundaryCandidate)) {
						return false;
					}
				}
			}
		}
		return true;
	}

	private String fetchNextTokenAfterPos(final String textWindow, final int[] boundaryCandidate) {
		int afterBoundaryPos = boundaryCandidate[0] + boundaryCandidate[1];
		int pos = textWindow.indexOf(" ", afterBoundaryPos);
		if (pos == afterBoundaryPos) {
			afterBoundaryPos++;
			pos = textWindow.indexOf(" ", afterBoundaryPos);
		}
		if (pos == afterBoundaryPos || pos == -1) {
			return null;
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
		if (postBoundaryToken == null) {
			return true;
		}
		for (Tuple2<Pattern, Boolean> rule : compiledPostRulesList) {
			final Matcher matcher = rule._1().matcher(postBoundaryToken);
			if (matcher.matches()) {
				return rule._2();
			}
		}
		return false;
	}

	private boolean checkPostList(String textWindow, int[] boundaryCandidate) {
		final String postBoundaryToken = fetchNextTokenAfterPos(textWindow, boundaryCandidate);
		if (postBoundaryToken == null) {
			return false;
		}
		for (String postBoundary : postList) {
			if (postBoundary.equals(postBoundaryToken)) {
				return true;
			}
		}
		return false;
	}

	private boolean checkPreRules(String textWindow, int[] boundaryCandidate) {
		final String token = textWindow.substring(boundaryCandidate[0], boundaryCandidate[0] + boundaryCandidate[1]);
		for (Tuple2<Pattern, Boolean> rule : compiledPostRulesList) {
			final Matcher matcher = rule._1().matcher(token);
			if (matcher.matches()) {
				return rule._2();
			}
		}
		return false;
	}

	private String fetchPreviousTokenBeforePos(final String textWindow, final int[] boundaryCandidate) {
		int firstPos = boundaryCandidate[0] - longestAbbrevation > 0 ? boundaryCandidate[0] - longestAbbrevation : 0;
		String candidateText = textWindow.substring(firstPos, boundaryCandidate[0] + boundaryCandidate[1]);
		String candidateAbbrevCandidate = candidateText.lastIndexOf(" ") != -1 ? candidateText.substring(candidateText.lastIndexOf(" ")) : candidateText;
		return candidateAbbrevCandidate.trim();
	}

	private boolean checkPreList(String textWindow, int[] boundaryCandidate) {
		String candidateAbbrevCandidate = fetchPreviousTokenBeforePos(textWindow, boundaryCandidate);
		for (String abbrivation : reversedAbbrevations) {
			if (abbrivation.trim().equals(candidateAbbrevCandidate)) {
				return true;
			}
		}
		return false;
	}
}
