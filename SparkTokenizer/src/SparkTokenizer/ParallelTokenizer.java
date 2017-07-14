package SparkTokenizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

public class ParallelTokenizer {

	public static void main(String[] args) {

		String inputFile = "";
		String outputPath = "";
		String dataPath = "";
		String timefileName = "";
		String[] evilSentenceEndCharacters = { "\"", "'", "“", "”", "„", "," };
		String[] punctuationCharacters = { ".", "!", "?", ";", "-", ":" };

		String runConfig = Long.toString(System.currentTimeMillis() / 1000);
		SparkConf conf = new SparkConf().setAppName("SparkTokenizer").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		conf.set("spark.hadoop.validateOutputSpecs", "false");// RewriteOutput

		System.out.println("first  arg :inputFile  /home/sahami/Documents/workspace/Data/input/tokenizer/example/example.txt");
		System.out.println("second arg :outputPath /home/sahami/Documents/workspace/Data/output/spark/Tokenizer");
		System.out.println("third arg  :dataPath   /home/sahami/Documents/workspace/Data/input/tokenizer");
		System.out.println("forth arg  :runConfig  run configuration, is used for runtime filename");
		if (args.length >= 4) {
			inputFile = args[0];
			outputPath = args[1];
			dataPath = args[2];
			runConfig = args[3];
			timefileName = "/u/sahami/workspace/runtime/TokenizerRT-" + runConfig + ".txt";

		} else {
			inputFile = "/home/sahami/Documents/workspace/Data/output/SentenceCleaner2ndBig10G/part-*";
			// "/home/sahami/Documents/workspace/Data/input/tokenizer/example/example.txt";
			dataPath = "/home/sahami/Documents/workspace/Data/input/tokenizer/";
			outputPath = "/home/sahami/Documents/workspace/Data/output/TokenizerBig10G/";
		}

		// read abbreviation list
		List<String> abbrevList = new ArrayList<String>();
		JavaRDD<String> objAbbrevList = sparkContext.textFile(dataPath + "/abbreviations/abbrev.txt");
		JavaRDD<String> abbrevRDD = objAbbrevList.map(new TrimOfWords(1));
		abbrevList = objAbbrevList.map(new TrimOfWords(1)).collect();
		// read explicit cut characters
		List<String> ExplicitCut = new ArrayList<String>();
		JavaRDD<String> tokenCharList = sparkContext.textFile(dataPath + "/100-wn-all.txt");
		JavaRDD<String> ExplicitCutRDD = tokenCharList.map(new readTokenizerChar()).map(new TrimOfWords(0)).filter(new removeUnwantedElementts());
		ExplicitCut = tokenCharList.map(new readTokenizerChar()).map(new TrimOfWords(0)).filter(new removeUnwantedElementts()).collect();
		// read fixed tokens
		List<String> fixedTokens = new ArrayList<String>();
		JavaRDD<String> fixedTokenList = sparkContext.textFile(dataPath + "/fixed_tokens.txt");
		JavaRDD<String> fixedTokensRDD = fixedTokenList.map(new TrimOfWords(0)).filter(new removeUnwantedElementts());
		fixedTokens = fixedTokenList.map(new TrimOfWords(0)).filter(new removeUnwantedElementts()).collect();
		// read tokenization character actions
		List<Tuple2<String, Integer[]>> tokenisationAction = new ArrayList<Tuple2<String, Integer[]>>();
		JavaRDD<String> tokenisationActionList = sparkContext.textFile(dataPath + "/tokenisation_character_actions.txt");
		JavaRDD<Tuple2<String, Integer[]>> tokenisationActionRDD = tokenisationActionList.map(new ReadTokenAction()).filter(new RemoveHeader());
		tokenisationAction = tokenisationActionList.map(new ReadTokenAction()).filter(new RemoveHeader()).collect();
		// read input file
		JavaRDD<String> textFile = sparkContext.textFile(inputFile);
		JavaRDD<Tuple2<String, Long>> sentences = textFile.map(new SplitMetaData());

		JavaRDD<Tuple2<String, Long>> tokenized = sentences
				.map(new TokenizerFunctionRDD(abbrevList, fixedTokens, tokenisationAction, evilSentenceEndCharacters, punctuationCharacters));

		JavaRDD<Tuple3<Integer, String, Long>> tokenizedWithLength = tokenized.map(new Function<Tuple2<String, Long>, Tuple3<Integer, String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple3<Integer, String, Long> call(Tuple2<String, Long> sentence) throws Exception {
				return new Tuple3<Integer, String, Long>(sentence._1.length(), sentence._1, sentence._2);
			}
		});

		// JavaRDD<Tuple2<String, Long>> tokenized = sentences
		// .map(new TokenizerFunction(abbrevList, ExplicitCut, fixedTokens,
		// tokenisationAction, evilSentenceEndCharacters,
		// punctuationCharacters));
		// JavaRDD<Tuple3<Integer, String, Long>> tokenizedWithLength =
		// tokenized.map(new Function<Tuple2<String, Long>, Tuple3<Integer,
		// String, Long>>() {
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple3<Integer, String, Long> call(Tuple2<String, Long>
		// sentence) throws Exception {
		// return new Tuple3<Integer, String, Long>(sentence._1.length(),
		// sentence._1, sentence._2);
		// }
		// });

		tokenizedWithLength.saveAsTextFile(outputPath);
		// List<String> tmpList = new ArrayList<String>();
		// Long start = sparkContext.startTime();
		// long end = System.currentTimeMillis();
		// tmpList.add("Configuration: " + runConfig);
		// tmpList.add("Spark start time in millisec is: " + start);
		// tmpList.add("\nCurrent time in millisec is: " + end);
		// tmpList.add("\nRun time in Second is: " + (end - start) / 1000);
		//
		// sparkContext.parallelize(tmpList).saveAsTextFile(outputPath +
		// "/RunTime/");
		long start = sparkContext.startTime();
		long end = System.currentTimeMillis();
		System.out.println("Runtime for Tokenizer  " + (end - start) / 1000);
		// try {
		// FileWriter writer = new FileWriter(timefileName, true);
		// writer.append("graphId,method,runtime\n");
		// Long start = sparkContext.startTime();
		// long end = System.currentTimeMillis();
		// writer.append("Spark start time in millisec is: " + start);
		// writer.append("\nCurrent time in millisec is: " + end);
		// writer.append("\nRun time in Second is: " + (end - start) / 1000);
		// writer.flush();
		//
		// } catch (IOException e1) {
		// e1.printStackTrace();
		// }
	}
}
