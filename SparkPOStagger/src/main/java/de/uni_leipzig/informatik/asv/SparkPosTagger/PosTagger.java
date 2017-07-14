package de.uni_leipzig.informatik.asv.SparkPosTagger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PosTagger {

	public static void main(String[] args) {
		String inputFile = "";
		String outputPath = "";
		String runConfig = Long.toString(System.currentTimeMillis() / 1000);
		SparkConf conf = new SparkConf().setAppName("POStagger").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		conf.set("spark.hadoop.validateOutputSpecs", "false");// RewriteOutput

		if (args.length == 3) {
			inputFile = args[0];
			outputPath = args[1];
			runConfig = args[2];
		} else {
			inputFile = "/home/sahami/Documents/workspace/Data/output/Tokenizer/part-*";
			outputPath = "/home/sahami/Documents/workspace/Data/output/POStagger/";
		}
		final String timefileName = outputPath + "runtime/POStaggerRT-" + runConfig + ".txt";
		JavaRDD<String> textFile = sparkContext.textFile(inputFile);
		JavaRDD<Tuple2<String, Long>> sentences = textFile.map(new SplitMetaData());
		JavaRDD<Tuple2<String, Long>> taggedSentences = sentences.map(new stanfordTagger());
		taggedSentences.saveAsTextFile(outputPath);

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