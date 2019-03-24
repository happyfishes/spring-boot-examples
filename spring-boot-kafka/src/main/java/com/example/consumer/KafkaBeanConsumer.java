package com.example.consumer;

import com.alibaba.fastjson.JSON;
import com.example.constant.Topic;
import com.example.model.Programmer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @ClassName KafkaBeanConsumer
 * @Describe
 * @create 2019-03-22 14:25
 * @Version 1.0
 **/
@Component
@Slf4j
public class KafkaBeanConsumer {

    @KafkaListener(groupId = "beanGroup", topics = Topic.BEAN)
    public void consumer(ConsumerRecord<String, Object> record) {
        System.out.println("消费者收到消息: " + JSON.parseObject(record.value().toString(), Programmer.class));
    }
}
