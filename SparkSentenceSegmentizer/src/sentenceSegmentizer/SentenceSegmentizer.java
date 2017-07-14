package sentenceSegmentizer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import gnu.getopt.Getopt;
import scala.Tuple2;
import scala.Tuple3;

public class SentenceSegmentizer {

	static String inputFilePath;
	static String outputFilePath;
	static String resourceFilePath;
	static String encodingText;
	static String runConfig;
	static boolean uppercaseFirstLetterPreList = false;

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkSentenceSegmentizer");// .setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		conf.set("spark.hadoop.validateOutputSpecs", "false");// RewriteOutput
		String inputFile = "";
		String outputFile = "";
		String resourcePath = "";
		String encoding = "UTF-8";
		String runConfigs = "";
		boolean autoUppercaseFirstLetterPreList = false;

		if (args.length == 0) {
			printHelp();
			inputFile = "/home/sahami/Documents/workspace/Data/output/SentenceCleaner/Small/part-*";
			// "/home/sahami/Desktop/test";
			// "/home/sahami/Documents/workspace/Data/input/sentenceSegmentizer/input.txt";
			outputFile = "/home/sahami/Documents/workspace/Data/output/Segmentizer/";
			resourcePath = "/home/sahami/Documents/workspace/Data/input/sentenceSegmentizer/";
			encoding = "UTF-8";
			autoUppercaseFirstLetterPreList = false;
			runConfigs = Long.toString(System.currentTimeMillis() / 1000);
		} else {
			init(args);
			inputFile = inputFilePath;
			outputFile = outputFilePath;
			resourcePath = resourceFilePath;
			encoding = encodingText;
			autoUppercaseFirstLetterPreList = uppercaseFirstLetterPreList;
			runConfigs = runConfig;
		}
		int longestAbbrevation = 0;
		String boundariesFile = resourcePath + "boundariesFile.txt";
		String postListFile = resourcePath + "postList.txt";
		String postRulesFile = resourcePath + "postRules.txt";
		String preListFile = resourcePath + "preList.txt";
		String preRulesFile = resourcePath + "preRules.txt";
		final String timefileName = outputFile + "runtime/SentSegRT-" + runConfigs + ".txt";

		// load Sentence Boundaries
		List<String> boundaries = new ArrayList<String>();
		JavaRDD<String> boundariesTmp = sparkContext.textFile(boundariesFile);
		boundaries = boundariesTmp.collect();
		// boundaries = Files.readAllLines(Paths.get(boundariesFile));
		if (boundaries.isEmpty()) {
			boundaries.add(".");
			boundaries.add("!");
			boundaries.add("?");
		}
		// load post/pre rule/list files

		// 1.postBoundaryList
		System.out.println((new Date()) + ": Loading post-boundary list from: " + postListFile + ", encoding " + encoding);
		List<String> postList = new ArrayList<String>();
		JavaRDD<String> postListTmp = sparkContext.textFile(postListFile);
		postList = postListTmp.collect();
		System.out.println("Number of POST LIST :     " + postList.size());
		// postList = Files.readAllLines(Paths.get(postListFile));
		for (String line : postList) {
			if (line.equals(null)) {
				postList.remove(line);
			}
		}
		System.out.println("Number of POST LIST after remove :     " + postList.size());
		System.out.println((new Date()) + ": Done! loading post-boundary list from: " + postListFile);

		// 2.postBoundaryRule
		System.out.println((new Date()) + ": Loading post-boundary rules from: " + postRulesFile + ", encoding " + encoding);
		JavaRDD<String> postRules = sparkContext.textFile(postRulesFile);
		JavaRDD<Tuple2<Pattern, Boolean>> compiledPostRules = postRules.map(new LoadPatterns());
		List<Tuple2<Pattern, Boolean>> compiledPostRuleSet = compiledPostRules.filter(new removeNullRules()).collect();
		System.out.println("Number of POST RULES :     " + compiledPostRuleSet.size());
		System.out.println((new Date()) + ": Done! loading post-boundary rules from: " + postRulesFile);

		// 3.pretBoundaryList
		System.out.println((new Date()) + ": Loading pre-boundary list from: " + preListFile + ", encoding " + encoding);
		List<String> preList = new ArrayList<String>();
		// Trie preListTrie = new Trie();//TODO
		JavaRDD<String> preListTmp = sparkContext.textFile(preListFile);
		preList = preListTmp.collect();
		// preList = Files.readAllLines(Paths.get(preListFile));
		for (String line : preList) {
			if (line.equals(null)) {
				preList.remove(line);
			}
			// else{//TODO uppercase
			// preListTrie.insert(line);
			// }
		}
		if (autoUppercaseFirstLetterPreList) {
			preList = uppercaseFirstLetterPreList(preList);
		}
		System.out.println("Number of PRE LIST :     " + preList.size());
		// find longest abbreviation in preList
		longestAbbrevation = Collections.max(preList, Comparator.comparing(s -> s.length())).length();

		System.out.println((new Date()) + ": Done loading pre-boundary list from: ");

		// 4.preBoundaryRule
		System.out.println((new Date()) + ": Loading pre-boundary rules from: " + preRulesFile + ", encoding " + encoding);
		JavaRDD<String> preRules = sparkContext.textFile(preRulesFile);
		JavaRDD<Tuple2<Pattern, Boolean>> compiledPreRules = preRules.map(new LoadPatterns());
		List<Tuple2<Pattern, Boolean>> compiledPreRulesSet = compiledPreRules.filter(new removeNullRules()).collect();
		System.out.println("Number of PRE RULES :     " + compiledPreRulesSet.size());
		System.out.println((new Date()) + ": Done loading pre-boundary rules from: " + preRulesFile);

		// load input file
		JavaRDD<String> cleanedSentencesWithMetadata = sparkContext.textFile(inputFile);

		// <line,metaDataIdx>
		JavaRDD<Tuple2<String, Long>> cleanedSentences = cleanedSentencesWithMetadata.map(new SplitMetaData());

		// <sent,metaDataIdx>
		JavaRDD<Tuple2<String, Long>> segmentedSentence = cleanedSentences
				.flatMap(new doSegmentation(preList, boundaries, compiledPostRuleSet, compiledPreRulesSet, postList, longestAbbrevation, encoding));

		JavaRDD<Tuple3<Integer, String, Long>> segmentedSentenceWithLength = segmentedSentence.map(new Function<Tuple2<String, Long>, Tuple3<Integer, String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple3<Integer, String, Long> call(Tuple2<String, Long> sentence) throws Exception {
				return new Tuple3<Integer, String, Long>(sentence._1.length(), sentence._1, sentence._2);
			}
		});
		segmentedSentenceWithLength.saveAsTextFile(outputFile);
		System.out.println(new Date() + "Done! Sentence Segmentizer finished. ");

		try {
			FileWriter writer = new FileWriter(timefileName, true);
			Long start = sparkContext.startTime();
			long end = System.currentTimeMillis();
			writer.append("Runtime for Sentence Segmentizer ");
			writer.append("\nSpark start time in millisec is: " + start);
			writer.append("\nCurrent time in millisec is:     " + end);
			writer.append("\nRun time in Second is:     " + (end - start) / 1000);
			writer.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		// try {
		// FileWriter fw;
		// fw = new FileWriter(timefileName);
		// BufferedWriter bw = new BufferedWriter(fw);
		// Long start = sparkContext.startTime();
		// long end = System.currentTimeMillis();
		// bw.write("Spark start time in millisec is: " + start);
		// bw.write("\nCurrent time in millisec is: " + end);
		// bw.write("\nRun time in Second is: " + (end - start) / 1000);
		// bw.close();
		// fw.close();
		// } catch (IOException e1) {
		// System.out.println("Not able to write the run time");
		// }
	}

	private static List<String> uppercaseFirstLetterPreList(List<String> preList) {
		List<String> upperCase = preList;
		for (String line : preList) {
			if (!Character.isUpperCase(line.charAt(0))) {
				preList.add(line.substring(0, 1).toUpperCase() + line.substring(1, line.length()));
			}
		}
		return upperCase;
	}

	private static void init(final String[] args) {
		Getopt g = new Getopt("SentenceSegmentizer", args, ":hi:o:r:e:u:x:");
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
			case 'r':
				resourceFilePath = g.getOptarg();
				break;
			case 'e':
				encodingText = g.getOptarg();
				break;
			case 'u':
				uppercaseFirstLetterPreList = true;
				break;
			case 'x':
				runConfig = g.getOptarg();
				break;
			}
		}
	}

	public static void printHelp() {
		System.out.println("Usage: java -jar SentenceSegmentizer.jar -i INPUT -o OUTPUT -r RESOURCE -e ENCODING [-u]");
		System.out.println("INPUT\t path to inputfile");
		System.out.println("OUTPUT\t path to outputfile");
		System.out.println("RESOURCE\t path to resource files");
		System.out.println("ENCODING\t for example UTF-8");
		System.out.println("u\t Uppercase First Letter PreList : default: false \n");
	}

}
