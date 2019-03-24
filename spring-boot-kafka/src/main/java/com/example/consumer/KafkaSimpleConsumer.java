package com.example.consumer;

import com.example.constant.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * @ClassName SimpleConsumer
 * @Describe 简单消息消费者
 * @create 2019-03-22 14:23
 * @Version 1.0
 **/
@Component
@Slf4j
public class KafkaSimpleConsumer {

    @KafkaListener(groupId = "simpleGroup", topics = Topic.SIMPLE)
    public void consumer1_1(ConsumerRecord<String, Object> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, Consumer consumer) {
        System.out.println("消费者收到消息: " + record.value() + ", topic: " + topic);
    }
}
