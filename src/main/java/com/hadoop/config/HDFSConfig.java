package com.hadoop.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Author:BYDylan
 * Date:2021/6/27
 * Description:
 */
@Slf4j
@Configuration
public class HDFSConfig {
    @Value("${hdfs.path}")
    private String hdfsPath;
    @Value("${hadoop.username}")
    private String userName;

    @Bean(name = "fileSystem")
    public FileSystem fileSystem() {
        FileSystem fs = null;
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        try {
            fs = FileSystem.get(new URI(hdfsPath), configuration, userName);
        } catch (IOException | InterruptedException | URISyntaxException e) {
            log.error("连接 hdfs 报错: {}", e.getMessage());
        }
        return fs;
    }
}
