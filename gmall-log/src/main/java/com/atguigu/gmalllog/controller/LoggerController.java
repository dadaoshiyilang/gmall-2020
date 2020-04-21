package com.atguigu.gmalllog.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("log")
    public String getLogger(@RequestParam("logString") String logString) {
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        String addTsLogString = jsonObject.toString();
        log.info(addTsLogString);
        if ("startup".equals(jsonObject.getString("type"))) {
            // 启动日志
            kafkaTemplate.send(GmallConstant.GMALL_STARTUP, addTsLogString);
        } else {
            // 事件日志
            kafkaTemplate.send(GmallConstant.GMALL_EVENT, addTsLogString);
        }
        return "success";
    }
}
