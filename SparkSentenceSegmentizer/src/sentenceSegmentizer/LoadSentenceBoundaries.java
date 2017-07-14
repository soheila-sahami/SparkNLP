package sentenceSegmentizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class LoadSentenceBoundaries {
	private Collection<String> sentenceBoundaries = new LinkedList<String>();
	private boolean isQuietMode = false;
	private String encoding = "UTF8";
	private static final String CR = "\n";

	private void initSentenceBoundaries() throws MalformedURLException {
		if (sentenceBoundaries.isEmpty()) {
			sentenceBoundaries.addAll(tryToLoadSentenceBoundariesFromFile());
			if (sentenceBoundaries.isEmpty()) {
				sentenceBoundaries.add(".");
				sentenceBoundaries.add("!");
				sentenceBoundaries.add("?");
				if (!isQuietMode) {
					System.out.println("No/Empty boundary candidates file found. Using defaults.");
				}
			}
			sentenceBoundaries.addAll(createNewLineBoundaries(sentenceBoundaries));
		}
	}

	private Collection<String> createNewLineBoundaries(final Collection<String> boundaries) {
		final Collection<String> newLineBoundaries = new LinkedList<String>();
		if (!boundaries.contains(CR)) {
			newLineBoundaries.add(CR);
		}
		return newLineBoundaries;
	}

	private List<String> tryToLoadSentenceBoundariesFromFile() throws MalformedURLException {
		final List<String> list = new LinkedList<String>();
		final URL boundariesFile = new File("boundariesFile.txt").toURI().toURL();
		if (boundariesFile != null) {
			try {
				if (!isQuietMode) {
					System.out.println((new Date()) + ": Loading boundary candiates from: " + boundariesFile
							+ " using encoding " + encoding);
				}
				final InputStreamReader fileReader = new InputStreamReader(boundariesFile.openStream());
				final BufferedReader bufferedReader = new BufferedReader(fileReader);
				while (bufferedReader.ready()) {
					final String line = new String(bufferedReader.readLine().getBytes(), encoding);
					list.add(line);
				}
				if (isQuietMode) {
					System.out.println((new Date()) + ": Done loading boundary candidates from: " + boundariesFile);
				}
			} catch (final FileNotFoundException e) {
				e.printStackTrace();
			} catch (final IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
}
