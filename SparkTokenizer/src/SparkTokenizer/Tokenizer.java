package SparkTokenizer;

public interface Tokenizer {
	final String DEFAULT_EXPLIZIT_CUT[] = new String[] { "(", ")", "{", "}", "[", "]", "\"", ":", "!", "?", "¿", ";",
			"'", ",", "#", "«", "»", "^", "`", "´", "¨", "‹", "›", "“", "”", "„", "‘", "‚", "’", "$", "€", "「", "」",
			"『", "』", "〈", "〉", "《", "》", "₩", "¢", "฿", "₫", "₤", "₦", "元", "₪", "₱", "₨", "-", "\\" };

	void setStrAbbrevListFile(String strAbbrevListFile);

	void setTOKENISATION_CHARACTERS_FILE_NAME(String TOKENISATION_CHARACTERS_FILE_NAME);

	void setFixedTokensFile(String fixedTokensFile);

	void init();

	String execute(String strLine);

	void setCharacterActionsFile(String string);
}