package com.example.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * @ClassName HadoopTemplate
 * @Describe
 * @create 2019-03-25 13:12
 * @Version 1.0
 **/
@Component
//@ConditionalOnBean(FileSystem.class)
@Slf4j
public class HadoopTemplate {

    @Autowired
    private FileSystem fileSystem;

    @Value("${hadoop.uri}")
    private String hadoop_uri;

    @Value("${hadoop.dir:/}")
    private String workDir;

    @PostConstruct
    public void init() {
        existDir(workDir, true);
    }

    public void uploadFile(String srcFile) {
        copyFileToHDFS(false, true, srcFile, workDir);
    }

    public void uploadFile(boolean del,String srcFile){
        copyFileToHDFS(del,true, srcFile, workDir);
    }

    public void uploadFile(String srcFile,String destPath){
        copyFileToHDFS(false,true, srcFile, destPath);
    }

    public void uploadFile(boolean del,String srcFile,String destPath){
        copyFileToHDFS(del,true, srcFile, destPath);
    }

    public void delFile(String fileName){
        rmdir(workDir, fileName) ;
    }

    public void delDir(String path){
        workDir = workDir + "/" +path;
        rmdir(path, null) ;
    }

    public void download(String fileName,String savePath){
        downloadFile(workDir +"/"+ fileName, savePath);
    }




    /**
     * 创建目录
     * @param filePath
     * @param create
     * @return
     */
    private Boolean existDir(String filePath, boolean create) {
        boolean flag = false;
        if (StringUtils.isEmpty(filePath)) {
            throw new IllegalArgumentException("filePath 不能为空！！！");
        }
        try {
            Path path = new Path(filePath);
            if (create) {
                if (!fileSystem.exists(path)) {
                    fileSystem.mkdirs(path);
                }
            }
            if (fileSystem.isDirectory(path)) {
                flag = true;
            }
        } catch (Exception e) {
            log.error("filePath cannot be created", e);
        }
        return flag;
    }

    /**
     * 文件上传 HDFS
     * @param delSrc       指是否删除源文件
     * @param overwrite    是否覆盖
     * @param srcFile      源文件，上传文件路径
     * @param destPath     hdfs 的目的路径
     */
    private void copyFileToHDFS(boolean delSrc, boolean overwrite, String srcFile, String destPath) {
        // 源文件路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/weibo.txt
        Path srcPath = new Path(srcFile);

        // 目的路径
        if (StringUtils.isNotBlank(hadoop_uri)) {
            destPath = hadoop_uri + destPath;
        }

        Path dstPath = new Path(destPath);
        // 实现文件上传
        try {
            // 获取FileSystem对象
            fileSystem.copyFromLocalFile(srcPath, dstPath);
            fileSystem.copyFromLocalFile(delSrc, overwrite, srcPath, dstPath);

            // 释放资源
//            fileSystem.close();
        } catch (Exception e) {
            log.error("File upload failed !!", e);
        }
    }

    /**
     * 从 HDFS 下载文件
     * @param hdfsFile
     * @param destPath
     */
    private void downloadFile(String hdfsFile, String destPath) {
        // 原文件路径
        if (StringUtils.isNotBlank(hadoop_uri)) {
            hdfsFile = hadoop_uri + hdfsFile;
        }

        Path hdfsPath = new Path(hdfsFile);
        Path dstPath = new Path(destPath);
        try {
            // 下载 hdfs 上的文件
            fileSystem.copyToLocalFile(hdfsPath, dstPath);

            // 释放资源
//            fileSystem.close();
        } catch (Exception e) {
            log.error("File download failed", e);
        }
    }

    /**
     * 删除文件或者文件目录
     * @param path
     * @param fileName
     */
    private void rmdir(String path, String fileName) {
        try {
            // 返回 FileSytem 对象
            if (StringUtils.isNotBlank(hadoop_uri)) {
                path = hadoop_uri + path;
            }
            if (StringUtils.isNotBlank(fileName)) {
                path = path + "/" + fileName;
            }
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            fileSystem.delete(new Path(path), true);
        } catch (IllegalArgumentException | IOException e) {
          log.error("File deletion failed", e);
        }
    }

    public String getNameSpace(){
        return workDir;
    }
}
