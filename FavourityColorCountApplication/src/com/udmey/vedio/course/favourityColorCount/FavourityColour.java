package com.udmey.vedio.course.favourityColorCount;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class FavourityColour {

	public static void main(String[] args) {

		Properties config = new Properties();
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "FavourityColour");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		
		
		String inputUserTopics = "UserFavourityColourInput1";
		String outputUserFavourityColour = "UserFavourityColourOutput";
		String intermediateopic = "UserFavourityTempTopic";

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> inputStreamColours = builder.stream(inputUserTopics);

		KStream<String, String> colourStream = inputStreamColours
				// filter bad value
				.filter((key,value)->value.contains(","))
				// assign new Key from values
				.selectKey((key, value) -> value.split(",")[0].toLowerCase())
				// convert values to lower
				.mapValues(colourValue -> colourValue.split(",")[1].toLowerCase())
				// filter bad colour
				.filter((key, value) -> Arrays.asList("red", "blue", "green").contains(value));
		// write to kafka intermediate topic
		colourStream.to(intermediateopic);
		
		// read from kafka as ktable
		KTable<String, Long> favouriteColourCounting = builder.table(intermediateopic)
				// group by
				.groupBy((key, value) -> new KeyValue(value, value))
				// count
				.count();

		// write back to kafka
		favouriteColourCounting.toStream().to(outputUserFavourityColour,Produced.with(Serdes.String(), Serdes.Long()));

		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		 // only do this in dev - not in prod
		streams.cleanUp();
		streams.start();

		System.out.println(streams.toString());

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
