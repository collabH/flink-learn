package dev.learn.flink.feature;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: ChangelogCheckpointFeature.java
 * @description: 开启changelog ck
 * @author: huangshimin
 * @date: 2023/1/11 2:20 PM
 */
public class ChangelogCheckpointFeature {
    public static void main(String[] args) {

        Configuration configuration=new Configuration();
        // 开启changelog state，默认关闭
        configuration.setBoolean("state.backend.changelog.enabled",true);
        // changelog最大运行失败次数，默认为3
        configuration.setInteger("state.backend.changelog.max-failures-allowed",3);
        // 定义以毫秒为单位执行状态后端定期持久化至文件系统的间隔。当该值为负值时，周期性物化将被禁用,默认10分钟一次
        configuration.setLong("state.backend.changelog.periodic-materialize.interval",10000L);
        // 定义changelog存储介质，默认为memory
        configuration.setString("state.backend.changelog.storage","memory");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }
}
