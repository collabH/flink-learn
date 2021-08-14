package dev.learn.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: FlinkEnvUtils.java
 * @description: FlinkEnvUtils.java类说明
 * @author: huangshimin
 * @date: 2021/8/14 7:40 下午
 */
public class FlinkEnvUtils {
    public static StreamExecutionEnvironment getStreamEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

}
