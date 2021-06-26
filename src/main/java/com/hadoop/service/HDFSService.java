package com.hadoop.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Author:BYDylan
 * Date:2020/5/1
 * Description:HDFS 请求demo
 */
@Slf4j
@Service
public class HDFSService {

    private FileSystem FS;

    public HDFSService(@Qualifier("fileSystem") FileSystem FS) {
        this.FS = FS;
    }

    public void mkdir(String hdfsDir) {
        boolean isMkdir = false;
        try {
            isMkdir = FS.mkdirs(new Path(hdfsDir));
        } catch (IOException e) {
            log.error("创建文件夹失败: {}", e.getMessage());
        }
        log.info("文件夹创建: {}", isMkdir ? "成功" : "失败");
    }

    void listStatus(String hdfsDir) {
        System.out.println("FS = " + FS);
        FileStatus[] fileStatuses = new FileStatus[0];
        try {
            fileStatuses = FS.listStatus(new Path(hdfsDir));
        } catch (IOException e) {
            log.info("获取路径下文件失败: {}", e.getMessage());
        }
        for (FileStatus fileStatus : fileStatuses) {
            log.info("路径下文件: {}", fileStatus.getPath().toString());
        }
    }

    public void rename(String oldFullPathName, String newFullPathName) {
        boolean isSuccess = false;
        try {
            isSuccess = FS.rename(new Path(oldFullPathName), new Path(newFullPathName));
        } catch (IOException e) {
            log.error("重命名 hdfs 文件失败: {}", e.getMessage());
        }
        log.error("重命名 hdfs 文件: {}", isSuccess ? "成功" : "失败!");
    }

    public void getTime(String hdfsFullPathName) {
        FileStatus fileStatus = null;
        try {
            fileStatus = FS.getFileStatus(new Path(hdfsFullPathName));
        } catch (IOException e) {
            log.error("获取文件时间报错: {}", e.getMessage());
        }
        long modificationTime = fileStatus.getModificationTime();
        String fileModificationTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(modificationTime)
                        , ZoneId.systemDefault()));
        log.info("文件修改时间: {}", fileModificationTime);
    }

    public void deleteFile(String hdfsFullPathName)  {
        boolean isSuccess = false;
        try {
            isSuccess = FS.delete(new Path(hdfsFullPathName), false);
        } catch (IOException e) {
            log.error("删除 hdfs 文件失败: {}",e.getMessage());
        }
        log.error("删除 hdfs 文件: {}", isSuccess ? "成功" : "失败!");
    }

    public void addFile(String hdfsFullPathName, String content) {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = FS.create(new Path(hdfsFullPathName));
            fsDataOutputStream.writeUTF(content);
        } catch (IOException e) {
            log.error("创建文件失败: {}", e.getMessage());
        } finally {
            try {
                fsDataOutputStream.flush();
                fsDataOutputStream.close();
            } catch (IOException e) {
                log.error("创建文件失败后,关闭流失败: {}", e.getMessage());
            }
        }
    }

    public void putFile(String localFullPathName, String hdfsFullPathName) {
        try {
            FS.copyFromLocalFile(new Path(localFullPathName), new Path(hdfsFullPathName));
        } catch (IOException e) {
            log.error("上传文件失败: {}", e.getMessage());
        }
    }

    public void checkDirExists(String hdfsFullPathName) {
        boolean isExists = false;
        try {
            isExists = FS.exists(new Path(hdfsFullPathName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("文件:{}", isExists ? "已存在" : "不存在");
    }
}