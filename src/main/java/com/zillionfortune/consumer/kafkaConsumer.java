package com.zillionfortune.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class kafkaConsumer {

	
	public static void main(String[] args) {
		Properties props = new Properties();
		 props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092,node4:9092,node5:9092");
		 props.put("group.id", "test");
		 props.put("enable.auto.commit", "true");
		 props.put("auto.commit.interval.ms", "1000");
		 props.put("session.timeout.ms", "30000");
		 props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		 consumer.subscribe(Arrays.asList("zb", "aaa"));
		 while (true) {
		     ConsumerRecords<String, String> records = consumer.poll(10);
		     for (ConsumerRecord<String, String> record : records)
		         System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
		 }
	}
}
