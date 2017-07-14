package sentenceSegmentizer;

import java.io.Serializable;
import java.util.Comparator;

public class FindLongestString implements Comparator<String>, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public int compare(String o1, String o2) {
		if (o1.length() > o2.length())
			return 1;
		if (o1.length() == o2.length())
			return 0;
		return -1;
	}
}