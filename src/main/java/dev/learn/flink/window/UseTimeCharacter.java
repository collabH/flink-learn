package dev.learn.flink.window;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: UseTimeCharacter.java
 * @description: 时间语义
 * @author: by echo huang
 * @date: 2020-08-30 19:10
 */
public class UseTimeCharacter {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    }
}
