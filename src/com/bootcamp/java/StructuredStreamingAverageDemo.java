package com.bootcamp.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

/*
 * This class is used structured streaming, it is reading a directory
 * constantly and finding the average of age of the given input.
 */
public final class StructuredStreamingAverageDemo {
  private static String INPUT_DIRECTORY = "C:\\data";

  public static void main(String[] args) throws Exception {
    System.out.println("Starting StructuredStreamingAverage job...");
    

    //1 - Start the Spark session
    SparkSession spark = SparkSession
      .builder()
      .master("local")
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .appName("StructuredStreamingAverage")
      .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    //2- Define the input data schema
    StructType personSchema = new StructType()
                                    .add("firstName", "string")
                                    .add("lastName", "string")
                                    .add("gender", "string")
                                    .add("age", "long");

    //3 - Create a Dataset representing the stream of input files
    Dataset<Person> personDS = spark
      .readStream()
      .schema(personSchema)
      .json(INPUT_DIRECTORY)
      .as(Encoders.bean(Person.class));
    System.out.println("data updated");

    
    //4 - Create a temporary table so we can use SQL queries       
    personDS.createOrReplaceTempView("people");

    String sql = "SELECT AVG(age) as average_age, gender FROM people GROUP BY gender";
    String countsql = "select count(*) from people";
    Dataset<Row> ageAverage = spark.sql(sql);
    Dataset<Row> count = spark.sql(countsql);

    //5 - Write the output of the query to the console
    StreamingQuery query = ageAverage.writeStream()
      .outputMode("complete")
      .format("console")
      .start();
    
    //5 - Write the the output of the query to the console
    StreamingQuery countQuery = count.writeStream()
      .outputMode("complete")
      .format("console")
      .start();

    query.awaitTermination();
    countQuery.awaitTermination();
    
  }
}