package com.bootcamp.java;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/*
 * This class used as a kafka consumer which will help to receive the data 
 * from kafka topic and store into file or HDFS path. 
 */
public class KafkaConsumerJavaDemo {

	private final static String TOPIC = "KafkaTopic"; // This is the topic name you create through CLI
	private final static String BOOTSTRAP_SERVERS = "localhost:9092"; // This is the host where your kafka server is running.
	private final static String filePath ="C:\\bootcampOutput\\KafkaOutput\\Output.txt"; // This is the output path.
	private final static String hdfsFilePath ="hdfs://localhost:50071//BT//kafkaoutput.txt";// This is HDFS path


	private static Consumer<String, String> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, Calendar.getInstance().getTime().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // This is used to read the topic from beginning
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props); // Creating a consumer object with properties
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC)); // Here we subscribe to the topic which we want to consume
		return consumer;
	}

	static void runConsumer() throws InterruptedException, IOException, URISyntaxException {
		final Consumer<String, String> consumer = createConsumer();
		final int giveUp = 100;   int noRecordsCount = 0;
//		BufferedWriter buffWriter = new BufferedWriter(new FileWriter(hdfsFilePath));
		Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs = FileSystem.get( new URI("hdfs://localhost:50071"), configuration );	
        Path file = new Path(hdfsFilePath);
        if ( hdfs.exists( file )) { 
        	hdfs.delete( file, true ); 
        }
        FSDataOutputStream fos = hdfs.create(file);
		while (true) {
			final ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
			if (consumerRecords.count()==0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp) break;
				else continue;
			}
			consumerRecords.forEach( record->{
				System.out.printf("Consumer Record:(%s)\n",record.value());
				//Write to a file
				try {
			        System.out.println("value is ="+record.value());
			        fos.writeBytes(record.value()+"\n");
//			        buffWriter.write( record.value() +"\n" );
				} catch (IOException e) {

				}
			});
			consumer.commitAsync();
		}
		consumer.close();
//		buffWriter.close();
		System.out.println("DONE");
	}


	public static void main(String... args) throws Exception {
		runConsumer();
	}

}
