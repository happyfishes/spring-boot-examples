package com.example.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName HiveJdbcTemplateController
 * @Describe 使用 JdbcTemplate 操作 Hive
 * @create 2019-03-27 1:11
 * @Version 1.0
 **/
@RestController
@RequestMapping("/hive2")
@Slf4j
public class HiveJdbcTemplateController {

    @Autowired
    @Qualifier("hiveDruidTemplate")
    private JdbcTemplate hiveDruidTemplate;

    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hiveJdbcTemplate;

    /**
     * 示例：创建新表
     */
    @RequestMapping("/table/create")
    public String createTable() {
        StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
        sql.append("user_sample");
        sql.append("(user_num BIGINT, user_name STRING, user_gender STRING, user_age INT)");
        sql.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' "); // 定义分隔符
        sql.append("STORED AS TEXTFILE"); // 作为文本存储

        log.info("Running: " + sql);
        String result = "Create table successfully...";
        try {
            // hiveJdbcTemplate.execute(sql.toString());
            hiveDruidTemplate.execute(sql.toString());
        } catch (DataAccessException dae) {
            result = "Create table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;

    }

    /**
     * 示例：将Hive服务器本地文档中的数据加载到Hive表中
     */
    @RequestMapping("/table/load")
    public String loadIntoTable() {
        String filepath = "/home/hadoop/user_sample.txt";
        String sql = "load data local inpath '" + filepath + "' into table user_sample";
        String result = "Load data into table successfully...";
        try {
            // hiveJdbcTemplate.execute(sql);
            hiveDruidTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Load data into table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;
    }

    /**
     * 示例：向Hive表中添加数据
     */
    @RequestMapping("/table/insert")
    public String insertIntoTable() {
        String sql = "INSERT INTO TABLE  user_sample(user_num,user_name,user_gender,user_age) VALUES(888,'Plum','M',32)";
        String result = "Insert into table successfully...";
        try {
            // hiveJdbcTemplate.execute(sql);
            hiveDruidTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Insert into table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;
    }

    /**
     * 示例：删除表
     */
    @RequestMapping("/table/delete")
    public String delete(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        String result = "Drop table successfully...";
        log.info("Running: " + sql);
        try {
            // hiveJdbcTemplate.execute(sql);
            hiveDruidTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Drop table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;
    }
}
