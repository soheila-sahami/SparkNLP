package sentenceSegmentizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

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

public class SentenceSegmentizerAsFirst {

	static String inputFilePath;
	static String outputFilePath;
	static String resourceFilePath;
	static String encodingText;
	static String runConfig;
	static boolean uppercaseFirstLetterPreList = false;
	static boolean metaDataIncluded = true;

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkSentenceSegmentizer").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		conf.set("spark.hadoop.validateOutputSpecs", "false");// RewriteOutput
		String inputFile = "";
		String outputFile = "";
		String resourcePath = "";
		String encoding = "UTF-8";
		String runConfigs = "";
		boolean autoUppercaseFirstLetterPreList = false;
		String timefileName = "";

		if (args.length == 0) {
			printHelp();
			inputFile = "/home/sahami/Documents/workspace/Data/input/AA_deu*";
			outputFile = "/home/sahami/Documents/workspace/Data/output/Segmentizer1stBig10G/";
			resourcePath = "/home/sahami/Documents/workspace/Data/input/sentenceSegmentizer/";
			encoding = "UTF-8";
			autoUppercaseFirstLetterPreList = false;
			runConfigs = Long.toString(System.currentTimeMillis() / 1000);
			timefileName = "/home/Documents/workspace/Data/output/runtime/SentSegRT-" + runConfigs + ".txt";
		} else {
			init(args);
			inputFile = inputFilePath;
			outputFile = outputFilePath;
			resourcePath = resourceFilePath;
			encoding = encodingText;
			autoUppercaseFirstLetterPreList = uppercaseFirstLetterPreList;
			runConfigs = runConfig;
			timefileName = "/u/sahami/workspace/runtime/SentSegRT-" + runConfigs + ".txt";
		}
		int longestAbbrevation = 0;
		String boundariesFile = resourcePath + "boundariesFile.txt";
		String postListFile = resourcePath + "postList.txt";
		String postRulesFile = resourcePath + "postRules.txt";
		String preListFile = resourcePath + "preList.txt";
		String preRulesFile = resourcePath + "preRules.txt";

		// load Sentence Boundaries

		JavaRDD<String> boundariesTmp = sparkContext.textFile(boundariesFile).filter((x) -> x != null);
		List<String> boundaries = new ArrayList<String>();
		boundaries = boundariesTmp.collect();
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
		postList = postListTmp.filter((x) -> x != null).collect();
		System.out.println((new Date()) + ": Done! loading post-boundary list from: " + postListFile);

		// 2.postBoundaryRule
		System.out.println((new Date()) + ": Loading post-boundary rules from: " + postRulesFile + ", encoding " + encoding);
		JavaRDD<String> postRules = sparkContext.textFile(postRulesFile);
		JavaRDD<Tuple2<Pattern, Boolean>> compiledPostRules = postRules.map(new LoadPatterns());
		List<Tuple2<Pattern, Boolean>> compiledPostRuleSet = compiledPostRules.filter((x) -> x._1() != null).collect();
		System.out.println((new Date()) + ": Done! loading post-boundary rules from: " + postRulesFile);

		// 3.pretBoundaryList
		System.out.println((new Date()) + ": Loading pre-boundary list from: " + preListFile + ", encoding " + encoding);
		List<String> preList = new ArrayList<String>();
		// Trie preListTrie = new Trie();//TODO
		JavaRDD<String> preListTmp = sparkContext.textFile(preListFile).filter((x) -> x != null);
		if (autoUppercaseFirstLetterPreList)
			preListTmp = preListTmp.map(new UppercaseFirstLetterPreList());
		preList = preListTmp.collect();
		longestAbbrevation = Collections.max(preList, Comparator.comparing(s -> s.length())).length();
		System.out.println((new Date()) + ": Done loading pre-boundary list from: ");

		// 4.preBoundaryRule
		System.out.println((new Date()) + ": Loading pre-boundary rules from: " + preRulesFile + ", encoding " + encoding);
		JavaRDD<String> preRules = sparkContext.textFile(preRulesFile);
		JavaRDD<Tuple2<Pattern, Boolean>> compiledPreRules = preRules.map(new LoadPatterns());
		List<Tuple2<Pattern, Boolean>> compiledPreRulesSet = compiledPreRules.filter(new removeNullRules()).collect();
		System.out.println("Number of PRE RULES :     " + compiledPreRulesSet.size());
		System.out.println((new Date()) + ": Done loading pre-boundary rules from: " + preRulesFile);

		// read text file
		Configuration hadoopConf = new Configuration();
		if (metaDataIncluded) {
			hadoopConf.set("textinputformat.record.delimiter", "<source>");
		} else {
			hadoopConf.set("textinputformat.record.delimiter", "\n");
		}
		JavaRDD<String> texts = sparkContext.newAPIHadoopFile(inputFile, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf).values().map((x) -> x.toString());
		// <metaData,text,index of text in source>
		// JavaPairRDD<String, Tuple2<String, Long>> lines =
		// texts.flatMapToPair(new createMetaData(metaDataIncluded));
		JavaPairRDD<String, Tuple2<String, Long>> lines;
		if (metaDataIncluded) {
			lines = texts.flatMapToPair(new ReadMetaData());
		} else {
			lines = texts.flatMapToPair(new ReadDocuments());
		}
		// make an RDD of sources and index <metaData,ID>
		JavaPairRDD<String, Long> sourceIndex = lines.flatMap(new createSourceIndex()).distinct().zipWithUniqueId().coalesce(5);
		sourceIndex.saveAsTextFile(outputFile + "SorceIndex");
		// <text,ID of metaData>
		JavaPairRDD<String, Tuple2<Tuple2<String, Long>, Long>> tempSentences = lines.join(sourceIndex);
		// <text,ID of metaData+Sentence Index >
		JavaRDD<Tuple2<String, Long>> sentences = tempSentences.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Tuple2<String, Long>, Long>>, Tuple2<String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Long>> call(Tuple2<String, Tuple2<Tuple2<String, Long>, Long>> line) throws Exception {
				String text = line._2._1._1;
				// index for each sentence is SourceID*10000000+SentenceIndex
				Long Index = (line._2._2 * 10000000) + line._2._1._2;
				Tuple2<String, Long> sentenceWitdID = new Tuple2<String, Long>(text, Index);
				return Arrays.asList(sentenceWitdID).iterator();
			}
		});
		// <sent,metaDataIdx>
		JavaRDD<Tuple2<String, Long>> segmentedSentence = sentences
				.flatMap(new doSegmentation(preList, boundaries, compiledPostRuleSet, compiledPreRulesSet, postList, longestAbbrevation, encoding));

		JavaRDD<Tuple3<Integer, String, Long>> segmentedSentenceWithLength = segmentedSentence.map(new Function<Tuple2<String, Long>, Tuple3<Integer, String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple3<Integer, String, Long> call(Tuple2<String, Long> sentence) throws Exception {
				return new Tuple3<Integer, String, Long>(sentence._1.length(), sentence._1, sentence._2);
			}
		});
		segmentedSentenceWithLength.saveAsTextFile(outputFile + "Segmentized");
		System.out.println(new Date() + "Done! Sentence Segmentizer finished. ");

		long start = sparkContext.startTime();
		long end = System.currentTimeMillis();
		System.out.println("Runtime for Sentence Segmentizer  " + (end - start) / 1000);
		// try {
		// FileWriter writer = new FileWriter(timefileName, true);
		// long start = sparkContext.startTime();
		// long end = System.currentTimeMillis();
		// writer.append("Runtime for Sentence Segmentizer ");
		// writer.append("\nSpark start time in millisec is: " + start);
		// writer.append("\nCurrent time in millisec is: " + end);
		// writer.append("\nRun time in Second is: " + (end - start) / 1000);
		// writer.flush();
		// } catch (IOException e1) {
		// e1.printStackTrace();
		// }
	}

	private static void init(final String[] args) {
		Getopt g = new Getopt("SentenceSegmentizer", args, ":hi:o:r:e:u:x:m");
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
			case 'm':
				metaDataIncluded = false;
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
