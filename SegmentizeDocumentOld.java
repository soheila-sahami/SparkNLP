package NLPpipeline;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class SegmentizeDocumentOld implements FlatMapFunction<Tuple2<Long, String>, Tuple2<String, Long>> {
	// JavaPairRDD<Long, Tuple2<String, String>>

	private static final long serialVersionUID = 1L;
	List<String> reversedAbbrevations;
	List<Tuple2<Pattern, Boolean>> compiledPostRulesList;
	List<Tuple2<Pattern, Boolean>> compiledPreRulesList;
	List<String> postList;

	List<String> boundaries;
	String encoding;
	int longestAbbrevation;
	int postBoundaryWindowSize = 20;
	private final static int offset = 1000000000;

	public SegmentizeDocumentOld(List<String> reversedAbbrevation, List<String> boundariesList,
			List<Tuple2<Pattern, Boolean>> compiledPostRuleSet, List<Tuple2<Pattern, Boolean>> compiledPreRuleSet,
			List<String> postListBoundaris, int longestAbbrevationLength, String encoding1) {
		reversedAbbrevations = reversedAbbrevation;
		boundaries = boundariesList;
		compiledPostRulesList = compiledPostRuleSet;
		compiledPreRulesList = compiledPreRuleSet;
		postList = postListBoundaris;
		encoding = encoding1;
		longestAbbrevation = longestAbbrevationLength;
		System.out.println("reversedAbbrevation: " + reversedAbbrevation.size() + "\nboundariesList: "
				+ boundariesList.size() + "\ncompiledPostRuleSet: " + compiledPostRuleSet.size()
				+ "\ncompiledPreRuleSet: " + compiledPreRuleSet.size() + "\npostListBoundaris:  "
				+ postListBoundaris.size() + "\nlongestAbbrevationLength : " + longestAbbrevationLength);
		System.out.println(new Date() + "Starting sentence segmentizer: ");
	}

	@Override
	public Iterator<Tuple2<String, Long>> call(Tuple2<Long, String> doc) throws Exception {
		Long sourceID = doc._1;
		String text = doc._2;
		int counter = 1;
		List<Tuple2<String, Long>> sentences = new ArrayList<Tuple2<String, Long>>();

		String[] lines = text.split("\n");
		for (int i = 0; i < lines.length; i++) {
			int posInWindow = 0;
			boolean completed = false;
			String line = lines[i];
			while (!completed) {
				int[] boundaryCandidate = findNextSentenceBoundary(line, posInWindow);
				if (boundaryCandidate[0] != -1) {
					if (isSentenceBoundary(line, boundaryCandidate)) {
						sentences.add(
								new Tuple2<String, Long>(line.substring(0, boundaryCandidate[0] + boundaryCandidate[1]),
										sourceID * offset + counter));
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
					sentences.add(new Tuple2<String, Long>(line, sourceID * offset + counter));
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
		// if (hasSpaceBeforeOrAfter(textWindow, boundaryCandidate)) {
		// int x = 0;
		if (checkPreList(textWindow, boundaryCandidate)) {
			return false;
		} else {
			if (!checkPreRules(textWindow, boundaryCandidate)) {
				return false;
			} else {
				if (checkPostList(textWindow, boundaryCandidate)) {
					return false;
				} else {
					if (!checkPostRules(textWindow, boundaryCandidate)) {
						return false;
					}
				}
			}
		}
		// }
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
			// return null;
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
		// if (postBoundaryToken == null) {
		// return true;
		// }
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
		// if (postBoundaryToken == null) {
		// return false;
		// }
		if (postBoundaryToken.equals(" ")) {
			return false;
		}
		if (postList.contains(postBoundaryToken)) {
			return true;
		}
		// for (String postBoundary : postList) {
		// if (postBoundary.equals(postBoundaryToken)) {
		// return true;
		// }
		// }
		return false;
	}

	private boolean checkPreRules(String textWindow, int[] boundaryCandidate) {
		// final String token = textWindow.substring(boundaryCandidate[0],
		// boundaryCandidate[0] + boundaryCandidate[1]);
		String candidateAbbrev = fetchPreviousTokenBeforePos(textWindow, boundaryCandidate);
		for (Tuple2<Pattern, Boolean> rule : compiledPreRulesList) {
			Matcher matcher = rule._1().matcher(candidateAbbrev);
			if (matcher.matches()) {
				return rule._2();
			}
		}
		// for (Tuple2<Pattern, Boolean> rule : compiledPreRulesList) {
		// Matcher matcher = rule._1().matcher(token);
		// if (matcher.matches()) {
		// return rule._2();
		// }
		// }
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
		if (reversedAbbrevations.contains(candidateAbbrev)) {
			return true;
		} else {
			return false;
		}
	}
}
