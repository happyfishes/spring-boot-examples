package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * @ClassName Programmer
 * @Describe
 * @create 2019-03-22 14:30
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Programmer implements Serializable {

    private String name;

    private int age;

    private float salary;

    private Date birthday;
}
