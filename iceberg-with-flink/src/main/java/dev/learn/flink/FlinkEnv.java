package dev.learn.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @fileName: FlinkEnv.java
 * @description: Flink Env
 * @author: huangshimin
 * @date: 2021/7/31 3:16 下午
 */
public class FlinkEnv {

    public static TableEnvironment getTableEnv() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build());
    }

    public static StreamExecutionEnvironment getStreamEnv() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        return environment;
    }
}