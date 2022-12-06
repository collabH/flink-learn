package dev.learn.flink.dimjoin;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.StandardCharsets;

/**
 * @fileName: HotCacheDimJoin.java
 * @description: 热缓存dim join
 * @author: huangshimin
 * @date: 2022/12/6 4:15 PM
 */
public class HotCacheDimJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3);
        source.map(new RichMapFunction<Integer, String>() {
            private Jedis jedis;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                String host = parameters.getString(ConfigOptions.key("redis.host")
                        .stringType().noDefaultValue());
                int port = parameters.getInteger(ConfigOptions.key("redis.port")
                        .intType().noDefaultValue());
                String password = parameters.getString(ConfigOptions.key("redis.password")
                        .stringType().noDefaultValue());
                GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
                JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, host, port, 1000, password);
                jedis = jedisPool.getResource();
            }

            @Override
            public String map(Integer key) throws Exception {
                // 流量小时这里可以拉取redis，流量大时可以配合ttl local cache缓解大量流量打到redis中
                String name = new String(jedis.get(key.toString().getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8);
                return key + ":" + name;
            }
        }).print("data");
        env.execute();
    }
}
