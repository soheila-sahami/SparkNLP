package SparkTokenizer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

public class ParallelTokenizerOld {

	public static void main(String[] args) {

		String inputFile = "";
		String outputPath = "";
		String dataPath = "";
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
		} else {
			inputFile = "/home/sahami/Documents/workspace/Data/output/Segmentizer/part-*";
			// "/home/sahami/Documents/workspace/Data/input/tokenizer/example/example.txt";
			dataPath = "/home/sahami/Documents/workspace/Data/input/tokenizer";
			outputPath = "/home/sahami/Documents/workspace/Data/output/Tokenizer/";
		}
		final String timefileName = outputPath + "runtime/TokenizerRT-" + runConfig + ".txt";
		Tokenizer tokenizer = new CharacterBasedWordTokenizerImpl();
		tokenizer.setStrAbbrevListFile(dataPath + "/abbreviations/abbrev.txt");
		tokenizer.setTOKENISATION_CHARACTERS_FILE_NAME(dataPath + "/100-wn-all.txt");
		tokenizer.setFixedTokensFile(dataPath + "/fixed_tokens.txt");
		tokenizer.setCharacterActionsFile(dataPath + "/tokenisation_character_actions.txt");
		tokenizer.init();

		JavaRDD<String> textFile = sparkContext.textFile(inputFile);
		JavaRDD<Tuple2<String, Long>> sentences = textFile.map(new SplitMetaData());
		JavaRDD<Tuple2<String, Long>> tokenized = sentences.map(new TokenizerMapFunction(tokenizer));

		JavaRDD<Tuple3<Integer, String, Long>> tokenizedWithLength = tokenized.map(new Function<Tuple2<String, Long>, Tuple3<Integer, String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple3<Integer, String, Long> call(Tuple2<String, Long> sentence) throws Exception {
				return new Tuple3<Integer, String, Long>(sentence._1.length(), sentence._1, sentence._2);
			}
		});
		tokenizedWithLength.saveAsTextFile(outputPath);
		try {
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
			e1.printStackTrace();
		}
	}
}
