package dev.flink.hudi.config;

import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
        executionEnvironment.setParallelism(4);
        return executionEnvironment;
    }

    /**
     * 获取流式table执行环境
     *
     * @return
     */
    public static StreamTableEnvironment getStreamTableEnv() {
        StreamExecutionEnvironment streamEnv = getStreamEnv();
        CheckpointConfig checkpointConfig = streamEnv.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(30000);
        checkpointConfig.setForceUnalignedCheckpoints(true);
        return StreamTableEnvironment.create(streamEnv,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    }

    public static TableEnvironment getBatchTableEnv() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build());
    }
}
