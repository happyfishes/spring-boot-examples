package com.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * @ClassName HbaseProperties
 * @Describe
 * @create 2019-03-22 16:35
 * @Version 1.0
 **/
//@Data
//@ConfigurationProperties(prefix = "hbase")
public class HbaseProperties {

    private Map<String, String> config;
}
