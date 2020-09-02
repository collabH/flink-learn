package dev.learn.flink.environment;

import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: UseEnvironment.java
 * @description: Flink执行环境学习
 * @author: by echo huang
 * @date: 2020-08-29 17:16
 */
public class UseEnvironment {
    public static void main(String[] args) {
        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("hadoop", 6123, 10, "test.jar");

        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(4);


        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        LocalEnvironment localEnvironment1 = ExecutionEnvironment.createLocalEnvironment();
        CollectionEnvironment collectionsEnvironment = ExecutionEnvironment.createCollectionsEnvironment();

    }
}
