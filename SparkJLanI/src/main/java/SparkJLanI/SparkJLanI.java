package SparkJLanI;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

public class SparkJLanI {

	public static void main(String[] args) {
		// String specialChars = null;
		String wordFilesPath = "hdfs:///user/sahami/LanguageIdentification/resources/wordlists/plain";
		String wordsSpilter = " ";

		// create a java spark context
		SparkConf conf = new SparkConf().setAppName("SparkLanguageIdentification").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		conf.set("spark.hadoop.validateOutputSpecs", "false");// RewriteOutput
		String inputFile = "";
		String outputFilePath = "";
		String runConfigs = "";

		List<String> defaultLanguages = new ArrayList<String>();
		defaultLanguages.add("deu");
		defaultLanguages.add("eng");
		defaultLanguages.add("ara");
		defaultLanguages.add("fra");
		defaultLanguages.add("por");
		defaultLanguages.add("rus");
		defaultLanguages.add("spa");

		List<String> inputLanguages = new ArrayList<String>();

		if (args.length == 0) {
			inputFile = "/home/sahami/Documents/workspace/Data/output/SentenceCleaner/Big/part-*";
			outputFilePath = "/home/sahami/Documents/workspace/Data/output/LangIdentification/Big/";
			runConfigs = Long.toString(System.currentTimeMillis() / 1000);
			wordFilesPath = "/home/sahami/Documents/workspace/SparkJLanI/resources/wordlists/plain";
			inputLanguages = defaultLanguages;
		} else {
			inputFile = args[0];
			wordFilesPath = args[1];
			outputFilePath = args[2];
			runConfigs = args[3];
			if (args.length > 4) {
				for (int i = 3; i < args.length; i++) {
					inputLanguages.add(args[i]);
				}
			}
		}

		final String timefileName = outputFilePath + "runtime/JLanIRT-" + runConfigs + ".txt";
		// start
		// word,langugae, prob
		JavaPairRDD<String, Tuple2<String, Double>> languages;
		try {
			// load word lists and frequencies
			languages = setupLanguge(inputLanguages, wordFilesPath, sparkContext);
			// find the minimum of probability in whole words of languages
			Tuple2<String, Tuple2<String, Double>> wordWithMinimumProb = languages.min(new findMinProbability());
			double minProb = wordWithMinimumProb._2._2 - 0.000001D;

			// read sentences
			JavaRDD<String> cleanedSentencesWithMetadataIdx = sparkContext.textFile(inputFile);
			// <line,metaData>
			JavaRDD<Tuple2<String, Long>> cleanedSentences = cleanedSentencesWithMetadataIdx.map(new SplitMetaDataIdx());
			// add unique id to each sentence
			// <<line,metaDataIdx>,lineID>
			JavaPairRDD<Tuple2<String, Long>, Long> sentencesWithId = cleanedSentences.zipWithUniqueId();
			// add number of words per sentence
			// <lineID <line,numOfWords,metaDataIdx>>
			JavaPairRDD<Long, Tuple3<String, Integer, Long>> sentenceIdLenght = sentencesWithId.mapToPair(new GetSentenceLength());
			// split each sentence to words,keeping sentence id and length
			// <word,<lineID,numOfWords>>
			JavaPairRDD<String, Tuple2<Long, Integer>> words = sentenceIdLenght.flatMapToPair(new SplitToWords(wordsSpilter));
			// join sentence words with languages word list
			// get the prob and lang from lang for each word in words
			// return word,<id, num of words in sentence> ,<prob,lang>
			JavaPairRDD<String, Tuple2<Tuple2<Long, Integer>, Tuple2<String, Double>>> joinedWithLang = words.join(languages);
			// change the key column from word to sentence id
			JavaRDD<Tuple5<String, Long, Integer, String, Double>> noKeyDataset = joinedWithLang.map(new ConvertKeyColumn());
			// set sentence ID as key
			// <sentence id,language,num of words in sentence><word,prob>
			JavaPairRDD<Tuple3<Long, String, Integer>, Iterable<Tuple2<String, Double>>> wordWithLanguageProb = noKeyDataset.mapToPair(new setSentenceIDasKey()).groupByKey();

			// <sentence id,language,prob>
			JavaRDD<Tuple3<Long, String, Double>> sentenceIdWithLanguageProb = wordWithLanguageProb.map(new ProbabilityOfSentence(minProb));
			// <sentence id,language,prob> as key/value pair
			JavaPairRDD<Long, Tuple2<String, Double>> sentenceIdLanguageProbPair = sentenceIdWithLanguageProb
					.mapToPair(new PairFunction<Tuple3<Long, String, Double>, Long, Tuple2<String, Double>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<Long, Tuple2<String, Double>> call(Tuple3<Long, String, Double> t) throws Exception {
							return new Tuple2<Long, Tuple2<String, Double>>(t._1(), new Tuple2<String, Double>(t._2(), t._3()));
						}
					});
			// id,lang,prop
			// find language with highest probability
			JavaPairRDD<Long, Tuple2<String, Double>> maxProbLang = sentenceIdLanguageProbPair.reduceByKey(new MaxProbLanguage());

			// sentence-Id, lang,prop, sentence,numOfWords,metaDataIdx
			JavaPairRDD<Long, Tuple2<Tuple2<String, Double>, Tuple3<String, Integer, Long>>> sentenceWithLanguageTmp = maxProbLang.join(sentenceIdLenght);
			// lang, Sentence,MeteDataID, Prob
			JavaPairRDD<String, Tuple3<String, Long, Double>> sentenceWithLanguage = sentenceWithLanguageTmp.mapToPair(new seperateLanguages());
			sentenceWithLanguage.saveAsTextFile(outputFilePath + "/Unseperated/");
			// sentenceWithLanguage.saveAsTextFile(outputFilePath);
			// JavaPairRDD<String, Iterable<Tuple3<String, Long, Double>>>
			// seperatedLang = sentenceWithLanguage.groupByKey();
			// seperatedLang.saveAsTextFile(outputFilePath + "/Seperated/");

		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			// Path runTime = Files.createFile(Paths.get(timefileName));
			FileWriter fw;
			fw = new FileWriter(timefileName);
			BufferedWriter bw = new BufferedWriter(fw);
			Long start = sparkContext.startTime();
			long end = System.currentTimeMillis();
			bw.write("Spark start time in millisec is: " + start);
			bw.write("\nCurrent time in millisec is:     " + end);
			bw.write("\nRun time in Second is:     " + (end - start) / 1000);
			bw.close();
			fw.close();
		} catch (IOException e1) {
			System.out.println("Not able to write the run time");
		}
		sparkContext.close();
	}

	private static JavaPairRDD<String, Tuple2<String, Double>> setupLanguge(List<String> inputLanguages, String wordFilesPath, JavaSparkContext sparkContext) throws IOException {

		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = fs.listStatus(new Path(wordFilesPath));
		Path[] allFiles = new Path[status.length];
		for (int i = 0; i < status.length; i++) {
			allFiles[i] = status[i].getPath();
		}
		HashMap<String, Path> selectedLanguages = listOfSelectedLanguagesFile(allFiles, inputLanguages);
		// initialize source words for first time
		List<Tuple2<String, Tuple2<String, Double>>> tempInitialize = Arrays
				.asList(new Tuple2<String, Tuple2<String, Double>>("no__lang", new Tuple2<String, Double>("no__lang", -0.01D)));
		JavaPairRDD<String, Tuple2<String, Double>> sourceWords = sparkContext.parallelizePairs(tempInitialize);
		if (!selectedLanguages.isEmpty()) {
			Iterator<Entry<String, Path>> iter = selectedLanguages.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Path> lang = iter.next();
				String langName = lang.getKey();
				Path langFile = lang.getValue();
				// read languagefile
				JavaRDD<String> textFile = sparkContext.textFile(langFile.toString());
				// get total frequency
				Double sum = Double.parseDouble(textFile.first());
				// split to word/frequent
				JavaPairRDD<String, Double> temp = textFile.mapToPair(new PairFunction<String, String, Double>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Double> call(String row) {
						Tuple2<String, Double> returnRow = new Tuple2<String, Double>(null, 0.0D);
						String[] temp = row.split("\\t");
						if (temp.length >= 2) {
							returnRow = returnRow.copy(temp[1], Double.parseDouble(temp[0]));

						} else {// first row
							returnRow = returnRow.copy(null, Double.parseDouble("0.0"));
						}
						return returnRow;
					}
				});
				// update to word/prob
				JavaPairRDD<String, Double> wordProb = temp.mapValues(new CalculateWordProbability(sum));

				// add language name to RDD
				JavaPairRDD<String, Tuple2<String, Double>> finalLang = wordProb.mapToPair(new AddLanguageName(langName));
				// union to other languages word/prob
				sourceWords = sourceWords.union(finalLang);
				iter.remove(); // avoids a ConcurrentModificationException
			}

		} else {
			System.out.println("No language is selected");
			System.exit(1);
		}
		sourceWords = sourceWords.filter(new Function<Tuple2<String, Tuple2<String, Double>>, Boolean>() {
			// remove zero or minus probs (from initialization)
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<String, Tuple2<String, Double>> word) {
				return (word._2._2 > 0);
			}
		});
		return (sourceWords);
	}

	private static HashMap<String, Path> listOfSelectedLanguagesFile(Path[] allFiles, List<String> inputLanguages) {
		HashMap<String, Path> selectedLanguages = new HashMap<String, Path>();
		if (allFiles.length > 0) {
			if (inputLanguages.isEmpty()) {// no default languages
				System.out.println("No selected or default language! Please select ;anguages...");
				System.exit(1);
			} else {
				for (Path lang : allFiles) {
					String languageName = lang.getName().substring(0, lang.getName().length() - ".words".length());
					if (inputLanguages.contains(languageName)) {
						selectedLanguages.put(languageName, lang);
					}
				}
			}
		} else {
			System.out.println("No language file has found!");
			System.exit(0);
		}
		return selectedLanguages;
	}
}
