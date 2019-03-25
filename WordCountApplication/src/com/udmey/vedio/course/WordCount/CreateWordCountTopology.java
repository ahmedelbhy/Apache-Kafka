package com.udmey.vedio.course.WordCount;

import java.util.Arrays;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class CreateWordCountTopology {
	public Topology createWordToplogy() {

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> stream = builder.stream("Input-topic1");

		KTable<String, Long> wordCount = stream.mapValues(textLine -> textLine.toLowerCase())
				.flatMapValues(textLine -> Arrays.asList(textLine.split(" "))).groupBy((key, word) -> word).count();

		wordCount.toStream().to("Output-topic1", Produced.with(Serdes.String(), Serdes.Long()));

		return builder.build();

	}

}
