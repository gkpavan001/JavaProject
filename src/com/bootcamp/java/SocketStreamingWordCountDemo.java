package com.bootcamp.java;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

//nc -l -p 9999 --> run this command on cmd before run the program

public class SocketStreamingWordCountDemo {
	
	public static void main(String[] args) throws StreamingQueryException {
	
	SparkSession spark = SparkSession
			  .builder()
			  .appName("Socket Streaming Word Count")
			  .master("local")
			  .getOrCreate();
	spark.sparkContext().setLogLevel("ERROR");
	
	// Create DataFrame representing the stream of input lines from connection to localhost:9999
	Dataset<Row> lines = spark
	  .readStream()
	  .format("socket")
	  .option("host", "localhost")
	  .option("port", 9999)
	  .load();

	// Split the lines into words
	Dataset<String> words = lines
	  .as(Encoders.STRING())
	  .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

	// Generate running word count
	Dataset<Row> wordCounts = words.groupBy("value").count().withWatermark("timestamp", "1 minute");
	
	
	StreamingQuery query = wordCounts.writeStream()
			  .outputMode("complete")
			  .format("console")
			  .start();

	query.awaitTermination();
	}

}
