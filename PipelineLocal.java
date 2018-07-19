package NLPpipeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import gnu.getopt.Getopt;
import scala.Tuple2;

public class PipelineLocal {

	static String inputFile;
	static String outputPath;
	static String resourceFilePath;
	static String dataPath = "";
	static String encodingText = "UTF-8";
	static String runConfig = "";
	static boolean uppercaseFirstLetterPreList = false;
	static boolean metaDataIncluded = true;
	static boolean saveMiddleResults = false;
	static boolean doSegmentizing = false;
	static boolean doCleaning = false;
	static boolean doTokeniizing = false;
	static String artificialSpace = "$$$";
	static int offset = 1000000000;
	static String seperator = "\n";

	static boolean saveWithSources = true;

	static String langCode = "";
	static String textType = "";
	static String inputType = "raw-text";
	static boolean showSummary = false;
	static boolean replaceStrings = false;
	static boolean exchangeOutput = false;
	static int inputColumn = -1;

	private final static Logger logger = Logger.getLogger(PipelineLocal.class.getName());

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Pipeline").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		if (args.length == 0) {
			inputFile = "/home/soheila/Documents/workarea/data/raw-data/AA_deu0012-shortend.txt";
			outputPath = "/home/soheila/Documents/workarea/data/output/pipeline/";
			resourceFilePath = "/home/soheila/Documents/workarea/data/DataNeededForTasksOnHDFS/pipeline/";
			encodingText = "UTF-8";
			runConfig = "local";
			metaDataIncluded = true;
			doSegmentizing = true;
			doTokeniizing = true;
			doCleaning = true;
			saveMiddleResults = true;
			inputType = "tab-separated";
			langCode = "";
			inputColumn = 0;
			textType = "";
			replaceStrings = true;
			exchangeOutput = true;
		} else {
			init(args);
		}

		int longestAbbrevationLength = 0;
		String boundariesFile = resourceFilePath + "/boundariesFile.txt";
		String postListFile = resourceFilePath + "/postList.txt";
		String postRulesFile = resourceFilePath + "/postRules.txt";
		String preListFile = resourceFilePath + "/preList.txt";
		String preRulesFile = resourceFilePath + "/preRules.txt";
		String abbreviationsFile = resourceFilePath + "/abbreviations.txt";
		String fixedTokensFile = resourceFilePath + "/fixed_tokens.txt";
		String tokenisationActionFile = resourceFilePath + "/tokenisation_character_actions.txt";
		String rulesPath = resourceFilePath + "/rules/";


		// 1. Sentence Boundaries
		List<String> boundaries = readBoundaries(sparkContext, boundariesFile);
		// logger.info("Loading sentence boundaries from: " + boundariesFile);
		// List<String> boundaries = sparkContext.textFile(boundariesFile, 1).filter((x)
		// -> x != null).collect();
		// if (boundaries.isEmpty()) {
		// boundaries.add(".");
		// boundaries.add("!");
		// boundaries.add("?");
		// }
		// logger.info("Done! loading sentence boundaries list.");

		// 2.postBoundaryList
		logger.info("Loading post-boundary list from: " + postListFile + ", encoding " + encodingText);
		List<String> postList = sparkContext.textFile(postListFile, 1)
				.filter((x) -> x != null)
				.collect();
		logger.info("Done! loading post-boundary list.");

		// 3.postBoundaryRule
		logger.info("Loading post-boundary rules from: " + postRulesFile + ", encoding " + encodingText);
		List<Tuple2<Pattern, Boolean>> compiledPostRuleSet = sparkContext.textFile(postRulesFile, 1)
				.map(new LoadPatterns())
				.filter((x) -> x._1() != null)
				.collect();
		logger.info("Done! loading post-boundary rules");

		// 4.pretBoundaryList
		logger.info("Loading pre-boundary list from: " + preListFile + ", encoding " + encodingText);
		JavaRDD<String> preListTmp = sparkContext.textFile(preListFile, 1)
				.filter((x) -> x != null)
				.map((x) -> x.trim());
		if (uppercaseFirstLetterPreList)
			preListTmp = preListTmp.map(line -> line.substring(0, 1).toUpperCase() + line.substring(1, line.length()));
		List<String> preList = preListTmp.collect();
		longestAbbrevationLength = Collections.max(preList, Comparator.comparing(s -> s.length())).length();
		logger.info("Done loading pre-boundary list");

		// 5.preBoundaryRule
		logger.info("Loading pre-boundary rules from: " + preRulesFile + ", encoding " + encodingText);
		List<Tuple2<Pattern, Boolean>> compiledPreRulesSet = sparkContext.textFile(preRulesFile, 1)
				.map(new LoadPatterns())
				.filter((x) -> x._1() != null)
				.collect();
		logger.info("Done loading pre-boundary rules");

		// 6.abbreviations
		logger.info("Loading abbreviations from: " + abbreviationsFile + ", encoding " + encodingText);
		List<String> abbreviationList = sparkContext.textFile(abbreviationsFile, 1)
				.filter((x) -> x != null)
				.map(x -> x.trim().toLowerCase() + ".")
				.collect();
		logger.info("Done loading abbreviat,\n" +
				"			boolean doCleaning, boolean doTokeniizing, String runConfig, String inputFile, String outputPathions");

		// 7.fixed tokens
		logger.info("Loading fixed tokens from: " + fixedTokensFile + ", encoding " + encodingText);
		List<String> fixedTokens = sparkContext.textFile(fixedTokensFile, 1).filter((x) -> x != null)
				.map(x -> x.trim())
				.filter(new RemoveUnwantedElementts())
				.collect();
		logger.info("Done loading pre-boundary list from: ");

		// 8.tokenization character actions
		logger.info("Loading tokenization character actions from: " + fixedTokensFile + ", encoding " + encodingText);
		List<Tuple2<String, Integer[]>> tokenisationAction = sparkContext.textFile(tokenisationActionFile, 1)
				.filter((x) -> x != null)
				.map(new ReadTokenAction())
				.filter(line -> !(line._1.equals("header")))
				.collect();
		logger.info("Done loading tokenization character actions");

		// read input file
		Configuration hadoopConf = new Configuration();
		if (metaDataIncluded) {
			logger.info("Input data includes meta data.");
			hadoopConf.set("textinputformat.record.delimiter", "<source>");
		} else {
			logger.info("Input data does not include meta data.");
			hadoopConf.set("textinputformat.record.delimiter", "\n");
		}

		// <text,MetaDataIdx>
		JavaPairRDD<String, Long> texts = sparkContext
				.newAPIHadoopFile(inputFile, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf).values()
				.map((x) -> x.toString())
				.zipWithUniqueId();

		// RDD of sources and index <metaData,ID>
		JavaPairRDD<Long, String> sourceIndex = JavaPairRDD.fromJavaRDD(sparkContext.emptyRDD());
		// RDD of <DocumentIndex, text>
		JavaPairRDD<Long, String> sentences;
		if (metaDataIncluded) {
			// <MetaDataIdx,<metaData,text>>
			JavaPairRDD<Long, Tuple2<String, String>> documents = texts
					.mapToPair(new ReadDocumentMetaData())
					.filter(doc -> doc._1 != -1);

			// Create an RDD of sources and index <metaData,ID>
			sourceIndex = documents.flatMapToPair(new CreateDocumentSourceIndex());

			// Create an RDD of <MetaDataIdx,text>
			sentences = documents.flatMapToPair(
					(x) -> new ArrayList<Tuple2<Long, String>>(Arrays.asList(new Tuple2<Long, String>(x._1, x._2._2)))
							.iterator());
		} else {
			// Create an RDD of <DocumentIdx,text>
			sentences = texts.flatMapToPair(new SplitDocumentToSentences(offset))
					.filter(doc -> doc._1 != -1);
		}
		JavaPairRDD<Long, String> segmentedSentences = sentences;
		if (doSegmentizing) {
			segmentedSentences = sentences
					.flatMapToPair(new SegmentizeDocument(preList, boundaries, compiledPostRuleSet, compiledPreRulesSet,
							postList, longestAbbrevationLength, encodingText, offset));
			if (saveMiddleResults || (!doCleaning && !doTokeniizing)) {
				if (metaDataIncluded) {
					segmentedSentences.mapToPair(new ExtrcactSourceID(offset))
							.groupByKey()
							.mapToPair(new ConcatAndSortDocs(seperator))
							.join(sourceIndex)
							.map(x -> x._2._2 + seperator + x._2._1)
							.saveAsTextFile(outputPath + "/Segmentized/");
				}
			} else {
				segmentedSentences.sortByKey()
						.saveAsTextFile(outputPath + "/Segmentized");
			}
			logger.info("Done! Sentence Segmentizer finished. ");
		}
		long segmentEnd = System.currentTimeMillis();

		JavaPairRDD<Long, String> cleanedLines = segmentedSentences;
		if (doCleaning) {
			// load general rules
			List<String> generalRules = sparkContext.textFile(rulesPath + "/general.rules")
					.filter(line -> line.length() != 0)
					.filter(line -> !(line.startsWith("#") || line.startsWith("//")))
					.collect();
			logger.info("Read the general rules.  " + generalRules.size() + " rules read.");

			// load text type rules
			List<String> textTypeRules = new ArrayList<String>();
			if (textType.length() > 0) {
				String textTypeFile = "/texttype_" + textType + ".rules";
				textTypeRules = sparkContext.textFile(rulesPath + textTypeFile)
						.filter(line -> line.length() != 0)
						.filter(line -> !(line.startsWith("#") || line.startsWith("//")))
						.collect();
				if (textTypeRules.size() > 0) {
					generalRules.addAll(textTypeRules);
				}
				logger.info("Read the rules for text type  " + textType + ". " + textTypeRules.size() + " rules read.");
			}
			// load language rules
			List<String> languageRules = new ArrayList<String>();
			if (langCode.length() > 0) {
				String languageFile = "/lang_" + langCode + ".rules";
				languageRules = sparkContext.textFile(rulesPath + languageFile)
						.filter(line -> line.length() != 0)
						.filter(line -> !(line.startsWith("#") || line.startsWith("//")))
						.collect();
				if (languageRules.size() > 0) {
					generalRules.addAll(languageRules);
				}
				logger.info("Read the rules for language " + langCode + ". " + languageRules.size() + " rules read.");
			}
			HashMap<Integer, SentenceFilter> filterMap = loadRuleFiles(generalRules, textTypeRules, languageRules);
			if (filterMap.isEmpty()) {
				logger.info("No rule is loaded");
				sparkContext.close();
				System.exit(0);
			}
			// load String replacements list
			HashMap<String, String> replacements = new HashMap<String, String>();
			if (replaceStrings) {
				List<String> stringReplacements = sparkContext.textFile(rulesPath + "/StringReplacements.list")
						.filter(line -> line.length() != 0)
						.collect();
				for (String line : stringReplacements) {
					String[] lineArray = line.split("\t");
					if (lineArray.length == 2)
						replacements.put(lineArray[0], lineArray[1]);
				}
				logger.info("Read the replacement list.  " + replacements.size() + " replacements read.");
			}
			// Start Cleaning
			cleanedLines = segmentedSentences.flatMapToPair(
					new CleanSentence(inputType, inputColumn, replaceStrings, replacements, exchangeOutput, filterMap))
					.filter(line -> line._2.length() > 0);

			if (saveMiddleResults || !doTokeniizing) {
				if (metaDataIncluded) {
					cleanedLines.mapToPair(new ExtrcactSourceID(offset)).groupByKey()
							.mapToPair(new ConcatAndSortDocs(seperator))
							.join(sourceIndex)
							.map(x -> x._2._2 + seperator + x._2._1)
							.saveAsTextFile(outputPath + "/Cleaned/");
				} else {
					cleanedLines.sortByKey()
							.saveAsTextFile(outputPath + "/Cleaned/");
				}
			}
			logger.info("Done! Cleaner finished. ");
		}
		long cleanEnd = System.currentTimeMillis();

		if (doTokeniizing) {
			JavaPairRDD<Long, String> tokenizedSentences = cleanedLines
					.flatMapToPair(new TokenizeSentence(abbreviationList, fixedTokens, preList, postList,
							compiledPreRulesSet, compiledPostRuleSet, tokenisationAction, artificialSpace));
			if (metaDataIncluded) {
				tokenizedSentences.mapToPair(new ExtrcactSourceID(offset))
						.groupByKey()
						.mapToPair(new ConcatAndSortDocs(seperator))
						.join(sourceIndex)
						.map(x -> x._2._2 + seperator + x._2._1)
						.saveAsTextFile(outputPath + "/Tokenised/");
			} else {
				tokenizedSentences.sortByKey()
						.saveAsTextFile(outputPath + "/Tokenised");
			}
			logger.info("Done! Tokeniser finished. ");
		}
		long tokenEnd = System.currentTimeMillis();

		List<String> runTimeInfoList = createRunTimeInfo(sparkContext.startTime(), segmentEnd, cleanEnd, tokenEnd);
		sparkContext.parallelize(runTimeInfoList, 1).saveAsTextFile(outputPath + "/RunTime/");
		logger.info("===============================================================================================");
		logger.info("Pipeline is finished.");
		logger.info("Segmentizer run-time in Second: " + (segmentEnd - sparkContext.startTime()) / 1000);
		logger.info("Cleaner run-time in Second: " + (cleanEnd - segmentEnd) / 1000);
		logger.info("Tokenizer run-time in Second: " + (tokenEnd - cleanEnd) / 1000);
		logger.info("Outputs are stored in folder " + outputPath);
		logger.info("===============================================================================================");
		sparkContext.close();
		System.out.println();
	}

	private static List<String> readBoundaries(JavaSparkContext sparkContext, String boundariesFile) {
		logger.info("Loading sentence boundaries from: " + boundariesFile);
		List<String> boundaries = sparkContext.textFile(boundariesFile, 1).filter((x) -> x != null).collect();
		if (boundaries.isEmpty()) {
			boundaries.add(".");
			boundaries.add("!");
			boundaries.add("?");
		}
		logger.info("Done! loading sentence boundaries list.");
		return boundaries;
	}

	private static List<String> createRunTimeInfo(long start, long segmentEnd, long cleanEnd, long tokenEnd) {

		List<String> runTimeInfoList = new ArrayList<String>();
		String pipeline = "Sentence Segmentizer";
		if (doCleaning)
			pipeline += " & Sentence Cleaner";
		if (doTokeniizing)
			pipeline += " & Tokenizer";

		runTimeInfoList.add(pipeline);
		runTimeInfoList.add("Configuration: " + runConfig);
		runTimeInfoList.add("Input Files: " + inputFile);
		runTimeInfoList.add("Spark start time in millisec: " + start);
		runTimeInfoList.add("End time of segmentizer in millisec: " + segmentEnd);
		runTimeInfoList.add("Segmentizer run-time in Second: " + (segmentEnd - start) / 1000);
		if (doCleaning) {
			runTimeInfoList.add(" ");
			runTimeInfoList.add("End time of cleaning in millisec: " + cleanEnd);
			runTimeInfoList.add("Sentence cleaner run-time in Second: " + (cleanEnd - segmentEnd) / 1000);
		}
		if (doTokeniizing) {
			runTimeInfoList.add(" ");
			runTimeInfoList.add("End time of tokenizer in millisec: " + tokenEnd);
			runTimeInfoList.add("Tokenizer run-time in Second: " + (tokenEnd - cleanEnd) / 1000);
		}
		runTimeInfoList.add("Output Path: " + outputPath);
		return runTimeInfoList;
	}

	private static void init(final String[] args) {
		Getopt g = new Getopt("SentenceSegmentizer", args, ":hi:o:r:e:fx:mtcsdbl:n:p:gu");
		int c;
		while ((c = g.getopt()) != -1) {
			switch (c) {
			case 'h':
				printHelp();
			case 'i':
				inputFile = g.getOptarg();
				break;
			case 'o':
				outputPath = g.getOptarg();
				break;
			case 'r':
				resourceFilePath = g.getOptarg();
				break;
			case 'e':
				encodingText = g.getOptarg();
				break;
			case 'f':
				uppercaseFirstLetterPreList = true;
				break;
			case 'x':
				runConfig = g.getOptarg();
				break;
			case 'm':
				metaDataIncluded = true;
			case 't':
				doTokeniizing = true;
				break;
			case 'c':
				doCleaning = true;
				break;
			case 's':
				doSegmentizing = true;
				break;
			case 'd':
				saveMiddleResults = true;
				break;
			case 'b':
				inputType = "tab-separated";
			case 'l':
				langCode = g.getOptarg();
				break;
			case 'n':
				inputColumn = Integer.parseInt(g.getOptarg());
				break;
			case 'p':
				textType = g.getOptarg();
				break;
			case 'u':
				replaceStrings = true;
				break;
			case 'g':
				exchangeOutput = true;
				break;
			}
		}
	}

	public static void printHelp() {
		// ":hi:o:r:e:fx:mtcsdbl:n:p:gu"
		logger.info("Please be sure you have set the following parameters:");
		logger.info("Parameters: -i INPUT -o OUTPUT -r RESOURCE -e ENCODING -x CONFIGURATION ");
		logger.info("[-m -c -t -s -d -b -l LANG_CODE -n COLUMN -p TEXTTYPE -g -u]");
		logger.info("INPUT\t Full path of input file.");
		logger.info("OUTPUT\t Full path of ouput folder to save the results.");
		logger.info("RESOURCE\t Full path of resource files.");
		logger.info("ENCODING\t default is UTF-8");
		logger.info("CONFIGURATION\t number of  cores, executers & memory, just for logs, Ex: Exec64-Core32-Mem64");
		logger.info("m\t Metadata is included in text.  default: false");
		logger.info("c\t Do Cleaning. default: false");
		logger.info("t\t Do Tokenization. default: false");
		logger.info("s\t Do Sentence Segmentation. default: false");
		logger.info("d\t Save the Middle Results (cleanining and tokenization). default: false");
		logger.info("b\t Input type is tab-separated. Default is raw-text");
		logger.info("LANG_CODE\t language code in ISO 639-3");
		logger.info(
				"COLUMN\t column number: treats input as tabulator separated file, checks only specified column, index starts with 0");
		logger.info("TEXTTYPE\t text type: web|news|wikipedia");
		logger.info("u\t Replace: replace HTML entities with UTF8 characters");
		logger.info("g\t Exchange: write the ill-formed sentences to output (+triggered rule)");
		logger.info("f\t uppercase the FirstLetterPreList");
	}

	private static HashMap<Integer, SentenceFilter> loadRuleFiles(List<String> generalRules, List<String> textTypeRules,
			List<String> languageRules) {
		// TODO re-implement RuleFileParser!not efficient in Spark
		HashMap<Integer, SentenceFilter> filterMap = new HashMap<Integer, SentenceFilter>();
		filterMap.putAll(RuleFileParser.parseRuleFile(generalRules, "General"));

		if (textTypeRules.size() > 0)
			filterMap.putAll(RuleFileParser.parseRuleFile(textTypeRules, textType));

		if (languageRules.size() > 0)
			filterMap.putAll(RuleFileParser.parseRuleFile(languageRules, langCode));
		if (filterMap != null) {
			if (inputColumn != -1) // input is tab separated file
				inputType = "tab-separated";
			else // input is Wortschatz raw text file
				inputType = "raw-text";
		}
		return (filterMap);
	}
}