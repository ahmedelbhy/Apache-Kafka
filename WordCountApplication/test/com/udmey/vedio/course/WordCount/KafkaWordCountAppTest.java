package com.udmey.vedio.course.WordCount;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(value = Lifecycle.PER_CLASS)
class KafkaWordCountAppTest {
	TopologyTestDriver testDriver;
	StringSerializer stringSerializer = new StringSerializer();
	ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(stringSerializer,
			stringSerializer);

	@BeforeAll
	public void setupTestDriver() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka-Word-Count-App-Test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		CreateWordCountTopology createWordCountTopology = new CreateWordCountTopology();
		testDriver = new TopologyTestDriver(createWordCountTopology.createWordToplogy(), props);
	}

	@AfterEach
	public void closeTestDriver() {
		testDriver.close();
	}

	public void puchNewRecordName(String value) {
		testDriver.pipeInput(recordFactory.create("Input-topic1", null, value));
	}

	public ProducerRecord<String, Long> ReadRecordName() {
		return testDriver.readOutput("Output-topic1", new StringDeserializer(), new LongDeserializer());
	}

	@Test
	public void checkwordCountIsCorrect() {
		String fristExample = "Test Kafka Stream Processing";
		puchNewRecordName(fristExample);
		OutputVerifier.compareKeyValue(ReadRecordName(), "test", 1L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "kafka", 1L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "stream", 1L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "processing", 1L);
		assertNull(ReadRecordName());

		String secondExample = "Test Kafka kafka Stream Processing";
		puchNewRecordName(secondExample);
		OutputVerifier.compareKeyValue(ReadRecordName(), "test", 2L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "kafka", 2L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "kafka", 3L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "stream", 2L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "processing", 2L);
		assertNull(ReadRecordName());
	}

	@Test
	public void CheckWordCountLowerCase() {
		String upperCase="KAFKA KaFKA KafKa";
		puchNewRecordName(upperCase);
		OutputVerifier.compareKeyValue(ReadRecordName(), "kafka", 1L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "kafka", 2L);
		OutputVerifier.compareKeyValue(ReadRecordName(), "kafka", 3L);
		assertNull(ReadRecordName());
	}

}
