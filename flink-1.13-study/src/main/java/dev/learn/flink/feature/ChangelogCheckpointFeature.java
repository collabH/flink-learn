package dev.learn.flink.feature;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @fileName: ChangelogCheckpointFeature.java
 * @description: 开启changelog ck
 * @author: huangshimin
 * @date: 2023/1/11 2:20 PM
 */
public class ChangelogCheckpointFeature {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        // 开启changelog state，默认关闭
        configuration.setBoolean("state.backend.changelog.enabled", false);
        // changelog最大运行失败次数，默认为3
        configuration.setInteger("state.backend.changelog.max-failures-allowed", 3);
        // 定义以毫秒为单位执行状态后端定期持久化至文件系统的间隔。当该值为负值时，周期性物化将被禁用,默认10分钟一次
        configuration.setLong("state.backend.changelog.periodic-materialize.interval", 10000L);
        // 定义changelog存储介质，默认为memory
        configuration.setString("state.backend.changelog.storage", "memory");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(1000);
        DataStreamSource<String> kafkaStream = environment.fromSource(KafkaSource.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics("changelog_state_topic")
//                        .setUnbounded(OffsetsInitializer.earliest()) 会导致flink Failed to initialize partition splits
//                        due to错误
                        .setClientIdPrefix("kafka_client_")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId("changelog_group_id")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(), WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(5000)),
                "changelog_kafka_source");
        kafkaStream.print();
        environment.execute();
    }
}
