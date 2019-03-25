package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
//@ComponentScan("com.example.config")
public class HadoopApplication {

    public static void main(String[] args) {
        SpringApplication.run(HadoopApplication.class, args);
    }

}
