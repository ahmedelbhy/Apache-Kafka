package com.udmey.vedio.course.userEnrichKafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class UserPuchaseProducer {
	public static String userPurchasetopic = "";
	public static String userDatatopic = "";

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "41258752");
		properties.put(ProducerConfig.RETRIES_CONFIG, "2");
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "2000");
		properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
		properties.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "241362");
		properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
		properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "5000");
		properties.put(ProducerConfig.LINGER_MS_CONFIG, "5000");
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		System.out.println("New user Data \n");
		producer.send(userDataRecord("ahmed", "xxxxxxxxxxxxxx")).get();
		producer.send(userPuchaseRecord("ahmed", "zzzzzzzzzzzzzzz")).get();

		Thread.sleep(100);

		System.out.println("Not Exist user Data \n");
		producer.send(userPuchaseRecord("ahmed albhy", "zzzzzzzzzzzzzzz")).get();
		Thread.sleep(100);

		System.out.println("Update user Data \n");
		producer.send(userDataRecord("ahmed", "ttttttttttttttttt")).get();
		producer.send(userPuchaseRecord("ahmed", "gggggggggggg")).get();
		Thread.sleep(100);

		System.out.println("Delete user Data before have purchase \n");
		producer.send(userDataRecord("albhy", "sssssssssss")).get();
		producer.send(userDataRecord("albhy", null)).get();
		producer.send(userPuchaseRecord("albhy", "aaaaaaaaa")).get();

		Thread.sleep(100);

		System.out.println("New user Data \n");
		producer.send(userDataRecord("aml", "xxxxxxxxzzzz")).get();
		producer.send(userPuchaseRecord("aml", "mmmmmmmm")).get();
		Thread.sleep(100);

		producer.close();
	}

	private static ProducerRecord<String, String> userDataRecord(String name, String data) {
		return new ProducerRecord<String, String>(userDatatopic, name, data);
	}

	private static ProducerRecord<String, String> userPuchaseRecord(String name, String data) {
		return new ProducerRecord<String, String>(userPurchasetopic, name, data);
	}
}
