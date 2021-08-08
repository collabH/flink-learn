package dev.learn.flink.feature;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: ExecutionMode.java
 * @description: ExecutionMode.java类说明
 * @author: huangshimin
 * @date: 2021/8/8 8:35 下午
 */
public class ExecutionModeFeature {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // 执行DAG不同，批处理可以更多地优化

        // 状态后端 1.流模式flink使用状态后端控制状态如何存储如何做ck  2.批处理忽略状态后端，相反，键操作的输入按键分组(使用排序)
        // ，然后依次处理键的所有记录。这允许同时只保留一个键的状态。当移动到下一个键时，给定键的状态将被丢弃。

        // 2.处理顺序，流模式无法保证顺序一旦达到就处理除非配合watermark等， 批处理能够保证顺序
    }
}
