package com.bootcamp.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

/*
 * This class used read data from kafka topic and process the data 
 * using Spark streaming and sql
 */
public final class KafkaStreamingEmpDemo {

  public static void main(String[] args) throws Exception {
    System.out.println("Starting KafkaConnectorStreaming job...");
    

    //1 - Start the Spark session
    SparkSession spark = SparkSession
      .builder()
      .master("local")
      .appName("Kafka Connector streaming")
      .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    //2- Define the input data schema
    StructType personSchema = new StructType()
                                    .add("number", "int")
                                    .add("name", "string")
                                    .add("salary", "int");

    //3 - Create a Dataset representing the stream of input files
    Dataset<Row> df = spark
    		  .readStream()
    		  .format("kafka")
    		  .option("kafka.bootstrap.servers", "localhost:9092")
    		  .option("subscribe", "mySQLIncrementData-employee")
    		  .option("startingOffsets", "earliest")
    		  .load();
    Dataset<Row> valueDF = df.selectExpr("CAST(value AS STRING)");
    valueDF.printSchema();
    
    Dataset<Row> empDS = valueDF.select(functions.from_json(valueDF.col("value"), personSchema).as("employee"));
    Dataset<Row> enrichedDS = empDS.selectExpr("employee.number", "employee.name", "employee.salary");
    
    enrichedDS.printSchema();

    //4 - Create a temporary table so we can use SQL queries       
    enrichedDS.createOrReplaceTempView("emp");

    String empSql = "select * from emp";
    String sumSql = "select sum(salary) from emp";
    Dataset<Row> fullDataSQL = spark.sql(empSql);
    Dataset<Row> sumSQL = spark.sql(sumSql);

    //5 - Write the output of the query to the console
    StreamingQuery empQuery = fullDataSQL.writeStream()
      .outputMode("append")
      .format("console")
      .start();
    
    //5 - Write the the output of the query to the console
    StreamingQuery sumQuery = sumSQL.writeStream()
      .outputMode("append")
      .format("console")
      .start();
    
    spark.streams().awaitAnyTermination();

  }
}