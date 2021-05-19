package com.zjt.learn.elasticsearch.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 功能：
 *
 * @Author zhaojiatao
 * @Date 2021-05-19 09:29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person {
    private Integer id;
    private String name;
    private Integer age;
    private Date birthDay;
}
