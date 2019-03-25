package com.udmey.vedio.course.ConsProd;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DemoProducer {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "41258752");
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "LZ4");
		properties.put(ProducerConfig.RETRIES_CONFIG, "2");
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "2000");
		properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
		properties.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "241362");
		properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
		properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "5000");
		properties.put(ProducerConfig.LINGER_MS_CONFIG, "5000");
		KafkaProducer<String, Float> producer = new KafkaProducer<String, Float>(properties);

		for (int i = 0; i < 2000; i++) {
			ProducerRecord<String, Float> producerRecord = new ProducerRecord<String, Float>("Input-topic1", "test" + i,
					(float) 321554.020);
			Future<RecordMetadata> record = producer.send(producerRecord);
		}

		producer.close();

	}

}
