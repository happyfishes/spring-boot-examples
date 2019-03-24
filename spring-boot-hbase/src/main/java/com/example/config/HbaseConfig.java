package com.example.config;

import com.example.util.HbaseUtil;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * @ClassName HbaseConfig
 * @Describe
 * @create 2019-03-22 16:31
 * @Version 1.0
 **/
@Configuration
public class HbaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String quorum;

    @Value("${hbase.zookeeper.port}")
    private String port;

    @Bean
    public HbaseUtil getHbaseService(){
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.port", port);
        return new HbaseUtil(conf);
    }

//    @Bean("admin")
//    public Admin createHbaseAdmin(org.apache.hadoop.conf.Configuration conf) throws IOException {
//        Connection connection = ConnectionFactory.createConnection(conf);
//        Admin admin = connection.getAdmin();
//        return  admin;
//    }
//
//    @Bean("connection")
//    public Connection connection(org.apache.hadoop.conf.Configuration conf) throws IOException {
//        Connection connection = ConnectionFactory.createConnection(conf);
//        return connection;
//    }
}
