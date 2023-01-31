package dev.learn.flink;

import com.alibaba.fastjson.JSON;

/**
 * @fileName: DynamicJsonParser.java
 * @description: DynamicJsonParser.java类说明
 * @author: huangshimin
 * @date: 2023/1/31 11:17
 */
public class DynamicJsonParser {

    public <T> T parser(String json, Class<T> responseBody) {
        return JSON.parseObject(json, responseBody);
    }
}
