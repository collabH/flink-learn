package dev.learn.flink.operator;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: CacheOperator.java
 * @description: CacheOperator.java类说明
 * @author: huangshimin
 * @date: 2022/11/18 7:13 PM
 */
public class CacheOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        DataStream<Integer> streamSource = streamEnv.fromElements(1, 2, 3, 4, 5, 6);

    }
}
