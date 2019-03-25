package com.example.controller;

import com.example.config.HadoopTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName HdfsController
 * @Describe
 * @create 2019-03-25 13:12
 * @Version 1.0
 **/
@RestController
public class HdfsController {

    @Autowired
    private HadoopTemplate hadoopTemplate;

    @RequestMapping("/create")
    public String mkdir(){
        hadoopTemplate.init();
        return "dir create success";
    }

    @RequestMapping("/upload")
    public String upload(@RequestParam String srcFile){
        hadoopTemplate.uploadFile(srcFile);
        return "file upload success";
    }

    @RequestMapping("/delFile")
    public String del(@RequestParam String fileName){
        hadoopTemplate.delFile(fileName);
        return "file delete success";
    }

    @RequestMapping("/download")
    public String download(@RequestParam String fileName,@RequestParam String savePath){
        hadoopTemplate.download(fileName,savePath);
        return "file download success";
    }
}
