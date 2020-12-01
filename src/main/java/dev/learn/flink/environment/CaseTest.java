package dev.learn.flink.environment;

import dev.learn.flink.function.CustomSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: CaseTest.java
 * @description: CaseTest.java类说明
 * @author: by echo huang
 * @date: 2020-08-29 18:15
 */
public class CaseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.addSource(new CustomSourceFunction())
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value+"1";
                    }
                }).print();
        executionEnvironment.execute();
    }
}
