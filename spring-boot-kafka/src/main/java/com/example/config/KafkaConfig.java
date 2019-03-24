package com.example.config;

import com.example.constant.Topic;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName KafkaConfig
 * @Describe
 * @create 2019-03-24 18:55
 * @Version 1.0
 **/
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic groupTopic() {
        // 指定主题名称，分区数量，和复制因子
        return new NewTopic(Topic.GROUP, 10, (short) 2);
    }
}
