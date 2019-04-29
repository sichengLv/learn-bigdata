package com.lv.learn.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static Properties props = new Properties();

    static {
        props.put("bootstrap.servers", "mas130:9092,mas130:9093,s131:9092");
        // 消费者组id
        props.put("group.id", "consumer-test");
        // 自动提交偏移量, 由下面的时间参加控制
        props.put("enable.auto.commit", "true");
        // 一秒提交一次
        props.put("auto.commit.interval.ms", "1000");
        // 心跳检测消费者组的进程是否还活着
        props.put("session.timeout.ms", "30000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅主题消息
        consumer.subscribe(Arrays.asList("test2"));

        while (true) {
            // 拉取消息, 如果没有消息则等待100ms
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

}
