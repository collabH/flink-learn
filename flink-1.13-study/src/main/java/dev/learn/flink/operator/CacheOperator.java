package dev.learn.flink.operator;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.datastream.CachedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: CacheOperator.java
 * @description: CacheOperator.java类说明
 * @author: huangshimin
 * @date: 2022/11/18 7:13 PM
 */
public class CacheOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        CachedDataStream<Integer> cachedDataStream = streamEnv.fromElements(1, 2, 3, 4, 5, 6).cache();

        cachedDataStream.print();
        cachedDataStream.invalidate();

        streamEnv.execute();
        cachedDataStream.print();
    }
}
