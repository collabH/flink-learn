package dev.learn;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @fileName: WordCount.java
 * @description: WordCount.java类说明
 * @author: by echo huang
 * @date: 2020/11/19 4:27 下午
 */
public class TestApplicationMode {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1, 2, 3, 4, 5, 6, 7)
                .print();

        env.execute();
    }
}
