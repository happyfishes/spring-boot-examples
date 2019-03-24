package com.example.controller;

import com.alibaba.fastjson.JSON;
import com.example.constant.Topic;
import com.example.model.Programmer;
import com.example.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

/**
 * @ClassName TestKafkaController
 * @Describe
 * @create 2019-03-22 14:22
 * @Version 1.0
 **/
@Slf4j
@RestController
public class TestKafkaController {

    @Autowired
    private KafkaProducer producer;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 发送消息体为基本类型的消息
     */
    @GetMapping("sendSimple")
    public void sendSimple() {
        producer.sendMessage(Topic.SIMPLE, "Hello spring boot kafka !");
    }

    /**
     *  发送消息体为 bean 的消息
     */
    @GetMapping("sendBean")
    public void sendBean() {
        Programmer programmer = new Programmer("xiaoming", 12, 21212.33f, new Date());
        producer.sendMessage(Topic.BEAN, JSON.toJSON(programmer).toString());
    }

    /***
     * 多消费者组、组中多消费者对同一主题的消费情况
     */
    @GetMapping("sendGroup")
    public void sendGroup() {
        for (int i = 0; i < 4; i++) {
            ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(Topic.GROUP, i % 4,"key", "hello group " + i);
            future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                @Override
                public void onFailure(Throwable throwable) {
                    log.error("发送消息失败: " + throwable.getMessage());
                }

                @Override
                public void onSuccess(SendResult<String, Object> sendResult) {
                    System.out.println("发送结果: " + sendResult.toString());
                }
            });
        }
    }
}
