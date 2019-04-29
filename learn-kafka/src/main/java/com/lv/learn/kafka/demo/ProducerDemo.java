package com.lv.learn.kafka.demo;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerDemo {

    private static Properties props = new Properties();

    static {
        props.put("bootstrap.servers", "mas130:9092,mas130:9093,mas131:9092");
        // 写入mmap之后立即返回Producer不调用flush叫异步
        props.put("producer.type", "async");
        // props.put("producer.type", "sync");

        // Leader和ISR中所有Follower都接收成功时确认
        props.put("acks", "all");
        props.put("retries", 3);
        // 16M
        props.put("batch.size", 16 * 1024);
        props.put("linger.ms", 1);
        // 1G
        props.put("buffer.memory", 1 * 1024 * 1024);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static void main(String[] args) {

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("demo-prd", Integer.toString(i), Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("Send Message Success!");
                }
            });
        }

        producer.close();
    }
}
