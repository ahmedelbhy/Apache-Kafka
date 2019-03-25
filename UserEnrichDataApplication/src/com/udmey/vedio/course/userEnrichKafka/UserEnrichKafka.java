package com.udmey.vedio.course.userEnrichKafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

public class UserEnrichKafka {
	public static void main(String[] args) {
		String userPurchaseTopic = "";
		String userDataTopic = "";
		String userPuchaceEnrichDataInnerJoinTopic = "";
		String userPuchaceEnrichDataLeftJoinTopic = "";
		Properties config = new Properties();

		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "Bank-Balance1");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		// we disable the cache to demonstrate all the "steps" involved in the
		// transformation - not recommended in prod
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> userPurchasekstream = builder.stream(userPurchaseTopic);

		GlobalKTable<String, String> globalKTable = builder.globalTable(userDataTopic);
		// inner Join
		KStream<String, String> userPurchaseEnrichDataInnerJoin = userPurchasekstream.join(globalKTable,
				(key, value) -> key,
				(userPuchace, userInfo) -> "userPuchaces" + userPuchace + "userInfo =[ " + userInfo + "]");

		userPurchaseEnrichDataInnerJoin.to(userPuchaceEnrichDataInnerJoinTopic);

		// left join
		KStream<String, Object> userPurchaseEnrichDataLeftJoin = userPurchasekstream.leftJoin(globalKTable,
				(key, value) -> key, (userPuchase, userInfo) -> {
					if (userInfo != null) {
						return "userPuchaces" + userPuchase + "userInfo= [ " + userInfo + "]";
					} else {
						return "userPuchaces" + userPuchase + "userInfo= [ Null ]";
					}
				});

		userPurchaseEnrichDataLeftJoin.to(userPuchaceEnrichDataLeftJoinTopic);

		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		
		streams.start();

		System.out.println(streams.toString());

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
