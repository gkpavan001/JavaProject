package com.bootcamp.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
 * This class is used to write data to Kafka topic from any chosen database. 
 */
public class MySQLToKafkaDemo {

	private static final String topicName = "EmpTopic"; // This is the topic name we use to produce the data
	private final KafkaProducer<Integer, String> producer;
	
	public MySQLToKafkaDemo(String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); // kafka broker
		props.put("client.id", "Bootcamp"); //An id string to pass to the server when making requests. 
		//The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging."
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<Integer, String>(props);
	}
	
	public void mySQLConn() throws Exception {
		
		String msg = null;
		String myDriver = "com.mysql.cj.jdbc.Driver";
		String url = "jdbc:mysql://192.168.19.1:3306/employee,pavan,1234";
		Class.forName(myDriver);
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/bootcamp","root","1234");
		String query = "select * from employee";
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(query);
		int key = 0;
		while(rs.next()) {
			int id = rs.getInt("number");
			String name = rs.getString("name");
			int sal = rs.getInt("salary");
			msg = id +","+name+","+sal;
			producer.send(
					new ProducerRecord<Integer, String>(topicName, key, msg)).get();
			System.out.println(key + "- " +msg);
			key ++;
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		MySQLToKafkaDemo mySQLToKafka = new MySQLToKafkaDemo(topicName);
		mySQLToKafka.mySQLConn();

	}

}
