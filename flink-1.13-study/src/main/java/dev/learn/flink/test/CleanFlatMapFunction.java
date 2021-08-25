package dev.learn.flink.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @fileName: CleanFlatMapFunction.java
 * @description: CleanFlatMapFunction.java类说明
 * @author: huangshimin
 * @date: 2021/8/25 10:08 下午
 */
public class CleanFlatMapFunction implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        out.collect("flatmap:" + value);
    }
}
