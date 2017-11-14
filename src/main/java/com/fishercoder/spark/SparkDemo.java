package com.fishercoder.spark;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by stevesun on 4/8/17.
 */
public class SparkDemo {
	public static void wordCountJava7(String filename) {
		System.out.println("In wordCountJava7...");
		JavaSparkContext javaSparkContext = getJavaSparkContext(filename);
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = javaSparkContext.textFile(filename);

		// Java 7 and earlier
		JavaRDD<String> words = input.flatMap(
				new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) {
						return Arrays.asList(s.split(" "));
					}
				});

		// Java 7 and earlier: transform the collection of words into pairs (word and 1)
		JavaPairRDD<String, Integer> counts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2(s, 1);
					}
				});

		// Java 7 and earlier: count the words
		JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});

		// Save the word count back out to a text file, causing evaluation.
		String path = "/tmp/" + System.currentTimeMillis();
		reducedCounts.saveAsTextFile(path);
		System.out.println("Saved result to this directory: " + path +
				"\n\n\n\n");
	}

	public static void wordCountJava8(String filename) {
		System.out.println("In wordCountJava8...");
		JavaSparkContext javaSparkContext = getJavaSparkContext(filename);
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = javaSparkContext.textFile(filename);

		// Java 8 with lambdas: split the input string into words
		JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")));
		String path = "/tmp/" + System.currentTimeMillis();
		words.saveAsTextFile(path);
		System.out.println("Saved words to this directory: " + path + "\n\n\n\n");

		// Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
		/**There might be redlines here, but it's totally fine to ignore. It's good to run.*/
		JavaPairRDD<String, Integer> counts = words.mapToPair
				(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> ((int) x + (int) y));

		// Save the word count back out to a text file, causing evaluation.
		path = "/tmp/" + System.currentTimeMillis();
		counts.saveAsTextFile(path);
		System.out.println("Saved counts to this directory: " + path + "\n\n\n\n");
	}

	public static void sparkAPI(String filename) {
		System.out.println("In sparkAPI...");
		JavaSparkContext javaSparkContext = getJavaSparkContext(filename);
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = javaSparkContext.textFile(filename);

		// Java 8 with lambdas: split the input string into words
		JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")));

		// Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
		/**There might be redlines here, but it's totally fine to ignore. It's good to run.*/
		JavaPairRDD<String, Integer> countsJavaPairRdd = words.mapToPair
				(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> ((int) x + (int) y));
		String path = "/tmp/" + System.currentTimeMillis();
		countsJavaPairRdd.saveAsTextFile(path);
		System.out.println("Saved countsJavaPairRdd result to this directory: " + path +
				"\n\n\n\n");

		/**Map JavaPairRDD back to JavaRDD:
		 * I just used a dummy logic to demo the map function.*/
		JavaRDD<String> javaRdd = countsJavaPairRdd.map(
				stringIntegerTuple2 -> {
					String word = stringIntegerTuple2._1();
					int count = stringIntegerTuple2._2();
					return word + ":" + count;
				}
		).filter(str -> Integer.parseInt(str.substring(str.indexOf(":") + 1)) > 1);
		/**Filter by those whose counts are greater than or equal to 1*/

		path = "/tmp/" + System.currentTimeMillis();
		javaRdd.saveAsTextFile(path);
		System.out.println("Saved javaRdd result to this directory: " + path +
				"\n\n\n\n");

		JavaPairRDD<String, Iterable<String>> groupedJavaRdd = javaRdd.groupBy(str -> str.substring(0, 1));
		path = "/tmp/" + System.currentTimeMillis();
		groupedJavaRdd.saveAsTextFile(path);
		System.out.println("Saved groupedJavaRdd result to this directory: " + path +
				"\n\n\n\n");

		JavaPairRDD<String, String> flatMappedJavaPairRdd = groupedJavaRdd.flatMapValues(
				values -> parseValues(values).values()
		);
		path = "/tmp/" + System.currentTimeMillis();
		flatMappedJavaPairRdd.saveAsTextFile(path);
		System.out.println("Saved flatMappedJavaPairRdd result to this directory: " + path +
				"\n\n\n\n");

		JavaRDD<String> flatMappedJavaPairRddValues = flatMappedJavaPairRdd.values();
		path = "/tmp/" + System.currentTimeMillis();
		flatMappedJavaPairRddValues.saveAsTextFile(path);
		System.out.println("Saved flatMappedJavaPairRddValues result to this directory: " + path +
				"\n\n\n\n");
	}

	private static Map<Integer, String> parseValues(Iterable<String> values) {
		Map<Integer, String> result = Maps.newHashMap();
		int i = 0;
		for (String str : values) {
			result.put(i++, str);
		}
		return result;
	}

	private static JavaSparkContext getJavaSparkContext(String appName) {
		// Define a configuration to use to interact with Spark
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(appName);

		// Create a Java version of the Spark Context from the configuration
		return new JavaSparkContext(sparkConf);
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Please specify input text path, e.g. input.txt and any other necessary parameters based on your code.");
		} else {
//			wordCountJava7(args[0]);
			wordCountJava8(args[0]);
			sparkAPI(args[0]);
		}
	}
}
