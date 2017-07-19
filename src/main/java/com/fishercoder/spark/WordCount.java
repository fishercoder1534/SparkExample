package com.fishercoder.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by stevesun on 4/8/17.
 */
public class WordCount {
	public static void wordCountJava7( String filename ) {
		System.out.println("In wordCountJava7...");
		// Define a configuration to use to interact with Spark
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = javaSparkContext.textFile( filename );

		// Java 7 and earlier
		JavaRDD<String> words = input.flatMap(
			new FlatMapFunction<String, String>() {
				public Iterable<String> call(String s) {
					return Arrays.asList(s.split(" "));
				}
			} );

		// Java 7 and earlier: transform the collection of words into pairs (word and 1)
		JavaPairRDD<String, Integer> counts = words.mapToPair(
			new PairFunction<String, String, Integer>(){
				public Tuple2<String, Integer> call(String s){
					return new Tuple2(s, 1);
				}
			} );

		// Java 7 and earlier: count the words
		JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
			new Function2<Integer, Integer, Integer>(){
				public Integer call(Integer x, Integer y){ return x + y; }
			} );

		// Save the word count back out to a text file, causing evaluation.
		String path = "/tmp/" + System.currentTimeMillis();
		reducedCounts.saveAsTextFile(path);
		System.out.println("Saved result to this directory: " + path + "." +
				"You can cd into this directory to view results.");
	}

	public static void wordCountJava8( String filename ) {
		System.out.println("In wordCountJava8...");
		// Define a configuration to use to interact with Spark
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = javaSparkContext.textFile( filename );

		// Java 8 with lambdas: split the input string into words
		JavaRDD<String> words = input.flatMap( s -> Arrays.asList( s.split( " " ) ) );

		// Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
		JavaPairRDD<String, Integer> counts = words.mapToPair
				(t -> new Tuple2( t, 1 ) ).reduceByKey((x, y) -> ((int) x + (int) y));

		// Save the word count back out to a text file, causing evaluation.
		String path = "/tmp/" + System.currentTimeMillis();
		counts.saveAsTextFile(path);
		System.out.println("Saved result to this directory: " + path + "." +
				"You can cd into this directory to view results.");
	}

	public static void main( String[] args )
	{
		if( args.length == 0 ) {
			System.out.println("args.length == 0");
			String arg = "input.txt";
//			wordCountJava7(arg);
			wordCountJava8(arg);
		} else {
			System.out.println("args.length != 0");
//			wordCountJava7(args[0]);
			wordCountJava8(args[0]);
		}
	}
}
