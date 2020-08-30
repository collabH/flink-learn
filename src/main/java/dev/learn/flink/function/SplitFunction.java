package dev.learn.flink.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @fileName: SplitFunction.java
 * @description: SplitFunction.java类说明
 * @author: by echo huang
 * @date: 2020-08-30 16:15
 */
public class SplitFunction implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] split = s.split(",");
        for (String s1 : split) {
            collector.collect(s1);
        }
    }
}
