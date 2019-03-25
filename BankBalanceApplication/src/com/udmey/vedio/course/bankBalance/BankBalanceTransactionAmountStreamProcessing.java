package com.udmey.vedio.course.bankBalance;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.scala.kstream.Grouped;
import org.apache.kafka.streams.state.KeyValueStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class BankBalanceTransactionAmountStreamProcessing {
	static String ouputTransactionBalance = "Transaction-Balance1";
	static String transactionTopic = "Transaction-Topic1";

	public static void main(String[] args) {

		Serializer<JsonNode> jsonSerializer = new JsonSerializer();
		Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
		Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

		Properties config = new Properties();

		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "Bank-Balance1");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// we disable the cache to demonstrate all the "steps" involved in the
		// transformation - not recommended in prod
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, JsonNode> banktransaction = builder.stream(transactionTopic,
				Consumed.with(Serdes.String(), jsonSerde));

		ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
		initialBalance.put("count", 0);
		initialBalance.put("balance", 0);
		initialBalance.put("name", "");
		initialBalance.put("time", Instant.ofEpochMilli(0L).toString());
	  
		KTable<String, JsonNode> custBalance = banktransaction.groupByKey(Grouped.with(Serdes.String(), jsonSerde))
				.aggregate(() -> initialBalance, (key, transaction, balance) -> newBalance(transaction, balance),
						Materialized.with(Serdes.String(), jsonSerde));

//		custBalance.toStream().to(ouputTransactionBalance, Produced.with(Serdes.String(), new SerdesTest<JsonNode>(JsonNode.class)));
//		custBalance.toStream().to(ouputTransactionBalance,
//				Produced.with(Serdes.String(), Serdes.serdeFrom(jsonSerializer, jsonDeserializer)));
		// TODO: test this   
		custBalance.toStream().to(ouputTransactionBalance,
				Produced.with(Serdes.String(), jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		// only do this in dev - not in prod
		// streams.cleanUp();
		streams.start();

		System.out.println(streams.toString());

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
		// create a new balance json object
		ObjectNode newBalance = JsonNodeFactory.instance.objectNode();

		newBalance.put("count", balance.get("count").asInt() + 1);

		newBalance.put("balance", balance.get("balance").asDouble() + transaction.get("Amount").asDouble());

		newBalance.put("name", transaction.get("name").asText());

		Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
		Long transactionEpoch = Instant.parse(transaction.get("Time").asText()).toEpochMilli();

		Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));

		newBalance.put("time", newBalanceInstant.toString());
		return newBalance;
	}
	 
}
