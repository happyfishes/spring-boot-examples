package com.example.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

/**
 * @ClassName HdfsConfig
 * @Describe
 * @create 2019-03-25 13:10
 * @Version 1.0
 **/
@Configuration
//@ConditionalOnProperty(name = "hadoop.uri")
@Slf4j
public class HdfsConfig {

    @Value("${hadoop.uri}")
    private String hadoop_uri;

    @Value("${hadoop.namespace}")
    private String namespace;

    @Value("${hadoop.namenode1}")
    private String nameNode1;

    @Value("${hadoop.namenode2}")
    private String nameNode2;

    /**
     * Configuration conf=new Configuration（）；
     * 创建一个Configuration对象时，其构造方法会默认加载hadoop中的两个配置文件，
     * 分别是hdfs-site.xml以及core-site.xml，这两个文件中会有访问hdfs所需的参数值，
     * 主要是fs.default.name，指定了hdfs的地址，有了这个地址客户端就可以通过这个地址访问hdfs了。
     * 即可理解为configuration就是hadoop中的配置信息。
     * @return
     */
    @Bean(name = "fileSystem")
    public FileSystem createFs() {
        // 读取配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", hadoop_uri);
        conf.set("dfs.nameservices", namespace);
        conf.set("dfs.ha.namenodes." + namespace, "nn1,nn2");
        conf.set("dfs.namenode.rpc-address." + namespace+".nn1", nameNode1);
        conf.set("dfs.namenode.rpc-address." + namespace+".nn2", nameNode2);
        //conf.setBoolean(name, value);
        conf.set("dfs.client.failover.proxy.provider." + namespace,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        // 文件系统
        FileSystem fs = null;
        // 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
        try {
            URI uri = new URI(hadoop_uri.trim());
            fs = FileSystem.get(conf);
        } catch (Exception e) {
            log.error("Get FileSystem Fail !!", e);
        }

        System.out.println("fs.defaultFS: " + conf.get("fs.defaultFS"));
        return fs;
    }
}
