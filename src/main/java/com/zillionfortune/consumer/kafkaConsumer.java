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
		 



//手动控制偏移量
//		 KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//	     consumer.subscribe(Arrays.asList("foo", "bar"));
//	     final int minBatchSize = 200;
//	     List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
//	     while (true) {
//	         ConsumerRecords<String, String> records = consumer.poll(100);
//	         for (ConsumerRecord<String, String> record : records) {
//	             buffer.add(record);
//	         }
//	         if (buffer.size() >= minBatchSize) {
//	             insertIntoDb(buffer);
//	             consumer.commitSync();
//	             buffer.clear();
//	         }
//	     }
	
//更精确的控制		 
//		 try {
//	         while(running) {
//	             ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
//	             for (TopicPartition partition : records.partitions()) {
//	                 List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//	                 for (ConsumerRecord<String, String> record : partitionRecords) {
//	                     System.out.println(record.offset() + ": " + record.value());
//	                 }
//	                 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//	                 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//	             }
//	         }
//	     } finally {
//	       consumer.close();
//	     }
		 
//订阅指定分区
//		 String topic = "foo";
//	     TopicPartition partition0 = new TopicPartition(topic, 0);
//	     TopicPartition partition1 = new TopicPartition(topic, 1);
//	     consumer.assign(Arrays.asList(partition0, partition1));
	}
}
