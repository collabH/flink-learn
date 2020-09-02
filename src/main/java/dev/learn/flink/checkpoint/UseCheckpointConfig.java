package dev.learn.flink.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: UseCheckpointConfig.java
 * @description: use checkpoint config
 * @author: by echo huang
 * @date: 2020/9/2 10:12 下午
 */
public class UseCheckpointConfig {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(10 * 60 * 1000);
        // 每次checkpoint间隔10s
        checkpointConfig.setMinPauseBetweenCheckpoints(10000);
        // 允许在有更近 savepoint 时回退到 checkpoint
        checkpointConfig.setPreferCheckpointForRecovery(true);
        // 容忍checkpoint的失败次数
        checkpointConfig.setTolerableCheckpointFailureNumber(0);
        // 当取消任务时，不会清空checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 最大并发checkpoint个数
        checkpointConfig.setMaxConcurrentCheckpoints(3);

        // fixme 设置重启策略
        // 固定延迟时间重启策略，在固定时间间隔内重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        // 失败率重启次数，失败间隔，延迟间隔
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(5), Time.seconds(10)));
    }
}
