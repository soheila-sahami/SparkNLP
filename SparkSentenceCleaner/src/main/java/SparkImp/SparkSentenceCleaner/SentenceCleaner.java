package SparkImp.SparkSentenceCleaner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import gnu.getopt.Getopt;
import scala.Tuple2;
import scala.Tuple3;

public class SentenceCleaner {

	// private static HashMap<Integer, SentenceFilter> filterMap = null;

	static String inputFilePath = "";
	static String outputFilePath = "";
	static String rulesFilePath = "";
	static String runConfig = Long.toString(System.currentTimeMillis() / 1000);
	static boolean metaDataIncluded = true;
	static String langCode = "UTF8";
	static String textType = "";
	static String inputType = "";
	static boolean verbose = false;
	static boolean showSummary = false;
	static boolean replaceStrings = false;
	static boolean exchangeOutput = false;
	static int inputColumn = -1;

	public static void printHelp() {
		System.out.println("Usage: java -jar SentenceCleaner.jar -i INPUT -o OUTPUT -p RULEPATH [-m] -x RunConfig [-l LANG_CODE] [-t TEXTTYPE] [-c COLUMN] [-r] [-v] [-e]");
		System.out.println("INPUT\t path to inputfile");
		System.out.println("OUTPUT\t path to outputfile");
		System.out.println("RULEPATH\t path to rule file");
		System.out.println("MetaData\t Meta data is inicluded in text file");
		System.out.println("RunConfig\t String to show run configuration, is used for runtime file name");
		System.out.println("LANG_CODE\t language code in ISO 639-3");
		System.out.println("TEXTTYPE\t text type: web|news|wikipedia");
		System.out.println("COLUMN\t column number: treats input as tabulator separated file, checks only specified column, index starts with 0");

		System.out.println("r\t replace: replace HTML entities with UTF8 characters");
		System.out.println("v\t verbose: verbose output");
		System.out.println("e\t exchange: write the ill-formed sentences to output (+triggered rule)");
	}

	private static void init(String[] args) {
		Getopt g = new Getopt("SentenceCleaner", args, "hi:l:t:o:p:m:x:c:vsre");
		int c;
		while ((c = g.getopt()) != -1) {
			switch (c) {
			case 'h':
				printHelp();
				System.exit(0);
			case 'i':
				inputFilePath = g.getOptarg();
				break;
			case 'o':
				outputFilePath = g.getOptarg();
				break;
			case 'p':
				rulesFilePath = g.getOptarg();
				break;
			case 'm':
				metaDataIncluded = true;
				break;
			case 'x':
				runConfig = g.getOptarg();
				break;
			case 'l':
				langCode = g.getOptarg();
				break;
			case 'c':
				inputColumn = Integer.parseInt(g.getOptarg());
				break;
			case 't':
				textType = g.getOptarg();
				break;
			case 'v':
				verbose = true;
				break;
			case 'r':
				replaceStrings = true;
				break;
			case 'e':
				exchangeOutput = true;
				break;
			}
		}
	}

	private static HashMap<Integer, SentenceFilter> loadRuleFiles(String rulesPath) {
		HashMap<Integer, SentenceFilter> filterMap = new HashMap<Integer, SentenceFilter>();

		if (new File(rulesPath).exists())
			filterMap.putAll(RuleFileParser.parseRuleFile(rulesPath, "General", verbose));

		String textTypeFile = "texttype_" + textType + ".rules";
		if (new File(textTypeFile).exists())
			filterMap.putAll(RuleFileParser.parseRuleFile(textTypeFile, textType, verbose));
		String languageFile = "lang_" + langCode + ".rules";
		if (new File(languageFile).exists())
			filterMap.putAll(RuleFileParser.parseRuleFile(languageFile, langCode, verbose));
		if (filterMap != null) {
			if (inputColumn != -1) // input is tab separated file
				inputType = "tab-separated";
			else // input is Wortschatz raw text file
				inputType = "raw-text";
		}
		return (filterMap);
	}

	public static Iterator<Tuple2<String, Long>> checkTabFile(Tuple2<String, Long> line, int inputColumn, Boolean replaceStrings, Boolean exchangeOutput,
			HashMap<Integer, SentenceFilter> filterMap) {
		List<Tuple2<String, Long>> finalOutput = new ArrayList<Tuple2<String, Long>>();
		boolean isValid = true;
		String[] lineArray;
		String[] output = null;
		int outputIndex = 0;
		lineArray = line._1.split("\t");
		if (inputColumn < lineArray.length & replaceStrings) {
			StringReplacements sr = new StringReplacements();
			lineArray[inputColumn] = sr.replaceEntities(lineArray[inputColumn]);
		}
		// sequential filter checks
		isValid = true;
		Iterator<Integer> iter = filterMap.keySet().iterator();
		while (iter.hasNext()) {
			SentenceFilter filter = filterMap.get(iter.next());
			if (inputColumn >= lineArray.length || !filter.sentenceIsValid(lineArray[inputColumn])) {
				isValid = false;
				if (exchangeOutput) { // write ill-formed sentences
					String outputLine = "";
					for (int i = 0; i < lineArray.length; i++)
						outputLine += lineArray[i] + "\t";
					if (outputLine.length() > 0)
						outputLine = outputLine.substring(0, outputLine.length() - 1);
					outputLine += "\tRule: " + filter.getFilterID() + " " + filter.getFilterDescription() + "\n";
					output[outputIndex++] = outputLine;
				}
				break;
			}
		}
		// write output
		if (isValid && !exchangeOutput) {
			String outputLine = "";
			for (int i = 0; i < lineArray.length; i++)
				outputLine += lineArray[i] + "\t";
			if (outputLine.length() > 0)
				outputLine = outputLine.substring(0, outputLine.length() - 1);
			outputLine += "\n";
			output[outputIndex++] = outputLine;
		}
		for (int i = 0; i < output.length; i++)
			finalOutput.add(new Tuple2<String, Long>(output[i], line._2));

		return (finalOutput.iterator());
	}

	public static Iterator<Tuple2<String, Long>> checkRawtextFile(Tuple2<String, Long> lineMetadata, Boolean replaceStrings, Boolean exchangeOutput,
			HashMap<Integer, SentenceFilter> filterMap) {
		List<Tuple2<String, Long>> finalOutput = new ArrayList<Tuple2<String, Long>>();
		boolean isValid = true;
		String[] output = { "" };
		int outputIndex = 0;
		String line = lineMetadata._1;
		if (replaceStrings) {
			StringReplacements sr = new StringReplacements();
			line = sr.replaceEntities(line);
		}
		// sequential filter checks
		isValid = true;
		Iterator<Integer> iter = filterMap.keySet().iterator();
		while (iter.hasNext()) {
			SentenceFilter filter = filterMap.get(iter.next());
			if (!filter.sentenceIsValid(line)) {
				isValid = false;
				if (exchangeOutput) // write ill-formed sentences
					output[outputIndex++] = (line + "\tRule: " + filter.getFilterID() + " " + filter.getFilterDescription());
				break;
			}
		}
		// write well-formed sentences
		if (isValid && !exchangeOutput)
			output[outputIndex++] = (line);
		for (int i = 0; i < output.length; i++)
			finalOutput.add(new Tuple2<String, Long>(output[i], lineMetadata._2));

		return (finalOutput.iterator());
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkSentenceCleaner").setMaster("local");
		conf.set("spark.hadoop.validateOutputSpecs", "false");// RewriteOutput
		@SuppressWarnings("resource")
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		Configuration hadoopConf = new Configuration();

		String inputFile = "";
		String outputDir = "";
		String rulesPath = "";
		String runConfigs = "";
		String timefileName = "";

		if (args.length == 0) {
			printHelp();
			inputFile = "/home/sahami/Documents/workspace/Data/output/Segmentizer1stBig/Segmentized/part*";
			// "hdfs:///user/sahami/SentenceCleaner/input-raw-small/";
			outputDir = "/home/sahami/Documents/workspace/Data/output/SentenceCleaner/BigTest2nd/";
			// "hdfs:///user/sahami/output/SentenceCleaneSmall";
			rulesPath = "/home/sahami/Documents/workspace/Data/input/general.rules";
			// "/u/sahami/workspace/data/general.rules";
			runConfigs = Long.toString(System.currentTimeMillis() / 1000);
			timefileName = "/home/sahami/Documents/workspace/Data/output/runtime/SentenceCleanerRT-" + runConfigs + ".txt";
		} else {
			init(args);
			inputFile = inputFilePath;
			outputDir = outputFilePath;
			rulesPath = rulesFilePath;
			runConfigs = runConfig;
			timefileName = "/u/sahami/workspace/runtime/SentenceCleanerRT-" + runConfigs + ".txt";
		}

		// load general rules
		HashMap<Integer, SentenceFilter> filterMap = loadRuleFiles(rulesPath);

		// read text file
		if (metaDataIncluded) {
			hadoopConf.set("textinputformat.record.delimiter", "<source>");
		} else {
			hadoopConf.set("textinputformat.record.delimiter", "\n");
		}
		JavaRDD<String> texts = sparkContext.newAPIHadoopFile(inputFile, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf).values()
				.map(new Function<Text, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Text text) throws Exception {
						return text.toString();
					}
				});
		// <metaData,text>
		JavaPairRDD<String, String> lines = texts.flatMapToPair(new createMetaData(metaDataIncluded));
		// make an RDD of sources and index <metaData,ID>
		JavaPairRDD<String, Long> sourceIndex = lines.flatMap(new createSourceIndex()).distinct().zipWithUniqueId();
		sourceIndex.saveAsTextFile(outputDir + "SorceIndex");
		// <text,ID of metaData>
		JavaPairRDD<String, Tuple2<String, Long>> tempSentences = lines.join(sourceIndex);
		JavaRDD<Tuple2<String, Long>> sentences = tempSentences.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<String, Long>>, Tuple2<String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Long>> call(Tuple2<String, Tuple2<String, Long>> line) throws Exception {
				Tuple2<String, Long> sentenceWitdID = new Tuple2<String, Long>(line._2._1, line._2._2);
				return Arrays.asList(sentenceWitdID).iterator();
			}
		});
		if (inputType.equals("raw-text")) {
			JavaRDD<Tuple2<String, Long>> cleanedLines = sentences.flatMap(new FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
				private static final long serialVersionUID = 1L;

				public Iterator<Tuple2<String, Long>> call(Tuple2<String, Long> line) {
					return checkRawtextFile(line, replaceStrings, exchangeOutput, filterMap);
				}
			}).filter(new Function<Tuple2<String, Long>, Boolean>() {
				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<String, Long> line) {
					return (line._1.length() > 0);
				}
			});
			// cleanedLines.groupBy(new collectSimilarSourceLines());
			JavaRDD<Tuple3<Integer, String, Long>> cleanedLineWithLength = cleanedLines.map(new Function<Tuple2<String, Long>, Tuple3<Integer, String, Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple3<Integer, String, Long> call(Tuple2<String, Long> sentence) throws Exception {
					return new Tuple3<Integer, String, Long>(sentence._1.length(), sentence._1, sentence._2);
				}
			});
			cleanedLineWithLength.saveAsTextFile(outputDir);
			// cleanedLines.saveAsTextFile(outputDir);
		} else if (inputType.equals("tab-separated")) {
			JavaRDD<Tuple2<String, Long>> cleanedLines = sentences.flatMap(new FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
				private static final long serialVersionUID = 1L;

				public Iterator<Tuple2<String, Long>> call(Tuple2<String, Long> line) {
					return checkTabFile(line, inputColumn, replaceStrings, exchangeOutput, filterMap);
				}
			}).filter(new Function<Tuple2<String, Long>, Boolean>() {
				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<String, Long> line) {
					return (line._1.length() > 0);
				}
			});
			JavaRDD<Tuple3<Integer, String, Long>> cleanedLineWithLength = cleanedLines.map(new Function<Tuple2<String, Long>, Tuple3<Integer, String, Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple3<Integer, String, Long> call(Tuple2<String, Long> sentence) throws Exception {
					return new Tuple3<Integer, String, Long>(sentence._1.length(), sentence._1, sentence._2);
				}
			});
			cleanedLineWithLength.saveAsTextFile(outputDir);
		} else {
			System.out.println("Input type: " + inputType + " is invalid. Process stoped!");
			System.exit(0);
		}

		try {
			FileWriter writer = new FileWriter(timefileName, true);
			writer.append("Runtime for Sentence Cleaner");
			Long start = sparkContext.startTime();
			long end = System.currentTimeMillis();
			writer.append("\nSpark start time in millisec is: " + start);
			writer.append("\nCurrent time in millisec is:     " + end);
			writer.append("\nRun time in Second is:     " + (end - start) / 1000);
			writer.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	// public JavaRDD<String> readOriginalSourceFiles(String inputFile , boolean
	// metaDataIncluded){
	// if (metaDataIncluded) {
	// hadoopConf.set("textinputformat.record.delimiter", "<source>");
	// } else {
	// hadoopConf.set("textinputformat.record.delimiter", "\n");
	// }
	// JavaRDD<String> texts = sparkContext.newAPIHadoopFile(inputFile,
	// TextInputFormat.class, LongWritable.class, Text.class,
	// hadoopConf).values()
	// .map(new Function<Text, String>() {
	// private static final long serialVersionUID = 1L;
	//
	// @Override
	// public String call(Text text) throws Exception {
	// return text.toString();
	// }
	// });
	// return texts;
	// }
}
