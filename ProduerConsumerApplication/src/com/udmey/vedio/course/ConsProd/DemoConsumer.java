package com.udmey.vedio.course.ConsProd;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

public class DemoConsumer {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.FloatSerializer");
		properties.put("group.id", "1");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		
		List<String>topics=new ArrayList<String>();
//		topics.add("Input-topic1");
		topics.add("Output-topic1");
		
		KafkaConsumer<String, Float> consumer = new KafkaConsumer<String, Float>(properties);
		consumer.subscribe(topics);
		while (true) {
			ConsumerRecords<String , Float> records=consumer.poll(Duration.ofMillis(2));
			for (ConsumerRecord<String , Float> consumerRecord : records) {
				System.out.printf("offset: %s"+"Key: %s"+"Value; %0.2f",consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
				
			}
			try {
				consumer.commitAsync(new OffsetCommitCallback() {
					
					@Override
					public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
 						
					}
				});
			} catch (CommitFailedException e) {
				// TODO: handle exception
			}finally {
				try {
				consumer.commitSync();
				}finally {
					consumer.close();
				}
			}
		}
		
	}
}
