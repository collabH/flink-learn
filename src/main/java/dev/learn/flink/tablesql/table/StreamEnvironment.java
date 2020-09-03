package dev.learn.flink.tablesql.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: UseJoinOperator.java
 * @description: UseJoinOperator.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 9:55 下午
 */
public class StreamEnvironment {
    public static StreamTableEnvironment getEnv(StreamExecutionEnvironment env) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode().useBlinkPlanner().build();
        return StreamTableEnvironment.create(env, settings);
    }

    public static TableEnvironment getBatchEnv() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inBatchMode().build();
        return TableEnvironment.create(settings);
    }
}
