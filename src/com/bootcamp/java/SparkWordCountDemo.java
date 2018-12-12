package com.bootcamp.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/*
 * This spark class is used for wordcount. 
 */
public class SparkWordCountDemo {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext context = new JavaSparkContext(conf);
		context.setLogLevel("ERROR");
		JavaRDD<String> input = context.textFile("C:\\bootcamp\\WordCountFile.txt");
		JavaPairRDD<String, Integer> counts = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
				.reduceByKey((x, y) ->  x +  y)
				.sortByKey();
		counts.saveAsTextFile("C:\\Users\\pavan\\Desktop\\bootcampOutput\\sparkOutput1");
		
//		counts.saveAsTextFile("hdfs://localhost:50071//sparkOutput5");
		context.close();
	}

}
