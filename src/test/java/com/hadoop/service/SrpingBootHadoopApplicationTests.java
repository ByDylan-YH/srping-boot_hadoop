package com.hadoop.service;

import com.hadoop.SrpingBootHadoopApplication;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = SrpingBootHadoopApplication.class)
class SrpingBootHadoopApplicationTests {
    @Autowired
    private HDFSService hdfsService;

    @Test
    void testMkDir() {
        String hdfsPath = "/test01";
        hdfsService.mkdir(hdfsPath);
    }

    @Test
    void testListFile() {
        String hdfsPath = "/";
        hdfsService.listStatus(hdfsPath);
    }
}
