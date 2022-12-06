package dev.learn.flink.dimjoin;

import com.google.common.collect.Maps;
import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @fileName: LocalCacheDimJoin.java
 * @description: 本地缓存加载维度表数据
 * @author: huangshimin
 * @date: 2022/12/6 2:24 PM
 */
public class LocalCacheDimJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<Integer> primaryKeySource = env.fromElements(1, 2, 3, 4);
        primaryKeySource.map(new RichMapFunction<Integer, String>() {
            private final Map<Integer, String> localCache = Maps.newHashMap();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 加载维度表数据
                localCache.put(1, "spark");
                localCache.put(2, "flink");
                localCache.put(3, "hadoop");
                localCache.put(4, "hudi");
            }

            @Override
            public String map(Integer key) throws Exception {
                String name = localCache.getOrDefault(key, "NA");
                return key + ":" + name;
            }
        }).print("data");
        env.execute();
    }
}
