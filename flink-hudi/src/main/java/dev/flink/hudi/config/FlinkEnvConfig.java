package dev.flink.hudi.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.client.model.HoodieRowData;

/**
 * @fileName: FlinkEnvConfig.java
 * @description: FlinkEnvConfig.java类说明
 * @author: huangshimin
 * @date: 2021/11/18 5:00 下午
 */
public class FlinkEnvConfig {
    /**
     * 获取流式执行环境
     *
     * @return
     */
    public static StreamExecutionEnvironment getStreamEnv() {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        return executionEnvironment;
    }

    /**
     * 获取流式table执行环境
     *
     * @return
     */
    public static StreamTableEnvironment getStreamTableEnv() {
        StreamExecutionEnvironment streamEnv = getStreamEnv();
        return StreamTableEnvironment.create(streamEnv,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    }

}
