package com.lv.learn.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaUtil {

    public static Consumer<String, String> getConsumer(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "mas130:9092,mas130:9093,s131:9092");
        // 消费者组id
        props.put("group.id", groupId);
        // 自动提交偏移量, 由下面的时间参加控制
        props.put("enable.auto.commit", "true");
        // 一秒提交一次
        props.put("auto.commit.interval.ms", "1000");

        // 心跳检测消费者组的进程是否还活着
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(props);
    }

}
