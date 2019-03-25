package kafka.wordCountApplication;

import java.util.Arrays;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class WordCountApplication {

	public static void main1(String[] args) {
		final Serde<String> stringSerde = Serdes.String();
 		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textLines = builder.stream("Input-topic1", Consumed.with(stringSerde, stringSerde));
		
		KTable<String, Long> wordCounts = textLines
			    // Split each text line, by whitespace, into words.
			    .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
			 
			    // Group the text words as message keys
			    .groupBy((key, value) -> value)
			 
			    // Count the occurrences of each word (message key).
			    .count();
		
		// Store the running counts as a change log stream to the output topic.
		wordCounts.toStream().to("Output-topic1", Produced.with(Serdes.String(), Serdes.Long()));
		System.out.println("Start Stream");
			   
	}

}
