package com.bootcamp.java;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
 * This class is used to read data from input and file and store into kafka topic.
 */
public class KafkaProducerJavaDemo {
	private static final String topicName = "KafkaTopic"; // This is the topic name we use to produce the data
	public static final String fileName = "C:\\bootcamp\\Superstore.csv"; // This is the file we are reading/ producing data to Kafka Topic
	private final KafkaProducer<Long, String> producer;

	public KafkaProducerJavaDemo(String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); // kafka broker
		props.put("client.id", "Bootcamp"); //An id string to pass to the server when making requests. 
		//The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging."
		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<Long, String>(props);
	}

	public void sendMessage(Long key, String value) {
		try {
			producer.send(
					new ProducerRecord<Long, String>(topicName, key, value))
			.get();
			System.out.println("" + key + ":" + value + "");
			if(key % 1000 == 0) {
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	public static void main(String [] args){
		KafkaProducerJavaDemo producer = new KafkaProducerJavaDemo(topicName);
		long lineCount = 0;
		FileInputStream fis;
		BufferedReader br = null;
		try {
			fis = new FileInputStream(fileName);
			//Construct BufferedReader from InputStreamReader
			br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			while ((line = br.readLine()) != null) {
				lineCount++;
				producer.sendMessage(lineCount, line);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}

