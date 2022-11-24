package dev.learn.flink.operator;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: OperatorChain.java
 * @description: 算子链和资源组
 * @author: huangshimin
 * @date: 2022/11/24 9:07 PM
 */
public class OperatorChain {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<Integer> ds = streamEnv.fromElements(1, 2, 3, 4, 5);
        ds
                .filter(data -> data % 2 == 0)
                // 构建map->map新的operator chain
                .map(data -> data)
                .startNewChain()
                .map(data -> data + 1)
                .print();

        // disableChaining、
        ds
                .disableChaining();

        // config slot sharingGroup
        ds.filter(data -> data % 2 == 0)
                .map(data -> data).slotSharingGroup("test");
        // name&desc
        ds.filter(data -> data % 2 == 0)
                .name("filter").setDescription("过滤非偶数数据");
    }
}
