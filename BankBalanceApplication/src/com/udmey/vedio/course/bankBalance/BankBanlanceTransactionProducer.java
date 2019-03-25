package com.udmey.vedio.course.bankBalance;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

public class BankBanlanceTransactionProducer {
	static String transactionTopic = "Transaction-Topic1";

	public static void main(String[] args) throws Exception {

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

		ObjectMapper mapper = new ObjectMapper();

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		int i = 0;
		long time=100;
		while (true) {
			System.out.println("processing Batch " + i);
			try {
				producer.send(newTransaction("ahmed"));
				Thread.sleep(time);
				producer.send(newTransaction("ibrahem"));
				Thread.sleep(time);
				producer.send(newTransaction("albhy"));
				Thread.sleep(time);
				i++;
			} catch (Exception e) {
				break;
			}
		}
		producer.close();

	}

	public static ProducerRecord<String, String> newTransaction(String custName) {
		ObjectNode custtransaction = JsonNodeFactory.instance.objectNode();
		custtransaction.put("name", custName);
		custtransaction.put("Amount", ThreadLocalRandom.current().nextDouble(0, 100));
		custtransaction.put("Time", Instant.now().toString());
		System.out.println(custtransaction.toString());

		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(transactionTopic, custName,
				custtransaction.toString());
		return producerRecord;

	}

}
