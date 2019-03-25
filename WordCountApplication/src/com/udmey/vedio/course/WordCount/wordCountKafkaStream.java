package com.udmey.vedio.course.WordCount;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

public class wordCountKafkaStream {

	public static void main1(String[] args) {

		Properties config = new Properties();
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "Word_count");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		CreateWordCountTopology CreateWordCountTopology = new CreateWordCountTopology();

		KafkaStreams streams = new KafkaStreams(CreateWordCountTopology.createWordToplogy(), config);
//		streams.cleanUp();
		streams.start();

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		// Update:
		// print the topology every 10 seconds for learning purposes
		while (true) {
			System.out.println(streams.toString());
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				break;
			}

		}
	}
}
