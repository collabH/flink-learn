package dev.learn.flink.counter;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: UserCounter.java
 * @description: 使用计数器
 * @author: by echo huang
 * @date: 2020/9/1 5:02 下午
 */
public class UserCounter {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> datasource = env.fromElements(1, 2, 3, 4, 5, 6, 7);

    }
}
