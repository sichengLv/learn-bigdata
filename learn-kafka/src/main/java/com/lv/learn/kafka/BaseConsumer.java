package com.lv.learn.kafka;

import com.lv.learn.util.KafkaUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;

/**
 * kafka消费者基类
 *
 * @author lvsicheng
 * @date 2019-4-8 17:22:40
 */
public abstract class BaseConsumer {

    protected static final Logger LOGGER = Logger.getLogger(BaseConsumer.class);

    /**
     * 处理消息
     *
     * @throws Exception
     */
    public abstract void process(ConsumerRecords<String, String> records) throws Exception;


    public void execute(String topics, String groupId, Boolean autoCommit) {
        Consumer<String, String> consumer = KafkaUtil.getConsumer(groupId);
        consumer.subscribe(Arrays.asList(topics.split(",")));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                process(records);
                if (!autoCommit) {
                    consumer.commitAsync();
                }
            }
        } catch (Exception e) {
            LOGGER.error("处理消息异常", e);
        } finally {
            consumer.close();
        }
    }

}
