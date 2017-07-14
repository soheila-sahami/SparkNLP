package SparkTokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import bak.pcj.map.ObjectKeyIntMap;
import bak.pcj.map.ObjectKeyIntOpenHashMap;

public abstract class AbstractWordTokenizer implements Tokenizer, Serializable {
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = Logger.getLogger(AbstractWordTokenizer.class);

	protected String strAbbrevListFile = null;
	protected Set<String> objAbbrevList = null;
	protected String TOKENISATION_CHARACTERS_FILE_NAME = null;
	protected String fixedTokensFile = null;
	protected ObjectKeyIntMap objTokenisationCharacters = null;
	protected String EXPLIZIT_CUT[] = null;
	protected int intSizeOfDot = 0;

	/**
	 * Creates a new instance of AbstractWordTokenizer
	 */
	public AbstractWordTokenizer() {
	}

	public void init() {
		if ((strAbbrevListFile == null) || strAbbrevListFile.equals("")) {
			System.out.println("Property \"strAbbrevListFile\" not set.");
		}

		try {
			String strLine = null;
			objAbbrevList = new HashSet<String>();

			// check if abbrevFile exists
			if (new File(strAbbrevListFile).exists()) {
				// ok try to load it
				BufferedReader objReader = new BufferedReader(new FileReader(strAbbrevListFile));

				while ((strLine = objReader.readLine()) != null) {
					objAbbrevList.add(strLine.trim() + ".");
				}
				objReader.close();
			} else {
				// well there was none .. fair enough
				LOGGER.warn(
						"AbstractWordTokenizer Warning: Could not load abbrviation list: [" + strAbbrevListFile + "]");
			}

			if (TOKENISATION_CHARACTERS_FILE_NAME == null) {
				LOGGER.warn("ATTENTION: TOKENISATION_CHARACTERS_FILE_NAME not set. Using default characters.");
				EXPLIZIT_CUT = DEFAULT_EXPLIZIT_CUT;
			} else {
				if (!new File(TOKENISATION_CHARACTERS_FILE_NAME).exists()) {
					LOGGER.warn("ATTENTION: TOKENISATION_CHARACTERS_FILE_NAME not found: "
							+ TOKENISATION_CHARACTERS_FILE_NAME + " Using default characters.");
					EXPLIZIT_CUT = DEFAULT_EXPLIZIT_CUT;
				} else {
					objTokenisationCharacters = new ObjectKeyIntOpenHashMap();
					BufferedReader objReader = new BufferedReader(new FileReader(TOKENISATION_CHARACTERS_FILE_NAME));

					while ((strLine = objReader.readLine()) != null) {
						String strSplit[] = strLine.split("\t");

						if (strSplit.length == 2) {
							String strTokenisationCharacter = strSplit[1].trim();
							objTokenisationCharacters.put(strTokenisationCharacter, strTokenisationCharacter.length());
						} else {
							LOGGER.warn("Ignoring line (reading tokenisation characters): " + strLine);
						}
					}
					objReader.close();

					// remove some elements of this list
					objTokenisationCharacters.remove("%^%");
					objTokenisationCharacters.remove("%$%");
					objTokenisationCharacters.remove("%N%");
					objTokenisationCharacters.remove("_TAB_");
					objTokenisationCharacters.remove("_");
					objTokenisationCharacters.remove(".");

					EXPLIZIT_CUT = new String[objTokenisationCharacters.size()];
					Iterator<String> objKeyIterator = objTokenisationCharacters.keySet().iterator();
					int i = 0;
					while (objKeyIterator.hasNext()) {
						EXPLIZIT_CUT[i] = objKeyIterator.next();
						i++;
					}
				}
			}
		} catch (IOException e) {
			System.out.println("Error loading abbreviation list ");
		}

	}

	public String getStrAbbrevListFile() {
		return strAbbrevListFile;
	}

	public void setStrAbbrevListFile(String strAbbrevListFile) {
		this.strAbbrevListFile = strAbbrevListFile;
	}

	public String getTOKENISATION_CHARACTERS_FILE_NAME() {
		return TOKENISATION_CHARACTERS_FILE_NAME;
	}

	public void setTOKENISATION_CHARACTERS_FILE_NAME(String tOKENISATION_CHARACTERS_FILE_NAME) {
		TOKENISATION_CHARACTERS_FILE_NAME = tOKENISATION_CHARACTERS_FILE_NAME;
	}

	protected int getCharacterLength(String strCharacter) {

		if (strCharacter == null) {
			return 1;
		}

		int length = objTokenisationCharacters.get(strCharacter);
		if (length > 0) {
			return length;
		}

		return 0;
	}

	protected int getWhiteSpaceBeforeDot(int intPosDot, String strFragment) {
		int intResult = intPosDot - 1;

		while (intResult > 0) {
			if (Character.isWhitespace(strFragment.charAt(intResult))) {
				return intResult;
			}
			intResult--;
		}

		return 0;
	}

	public String execute(String strLine) {
		return null;
	}

	protected static ArrayList<int[]> getWhitespacePositions(String strCurLine) {
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
}
