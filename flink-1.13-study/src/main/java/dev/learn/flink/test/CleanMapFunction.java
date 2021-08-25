package dev.learn.flink.test;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @fileName: UdfFunctionTest.java
 * @description: UdfFunctionTest.java类说明
 * @author: huangshimin
 * @date: 2021/8/25 9:55 下午
 */
public class CleanMapFunction implements MapFunction<String, String> {

    @Override
    public String map(String value) throws Exception {
        return "clean:" + value;
    }
}
