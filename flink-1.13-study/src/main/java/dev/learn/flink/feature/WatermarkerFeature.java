package dev.learn.flink.feature;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;

/**
 * @fileName: WatermarkerFeature.java
 * @description: WatermarkerFeature.java类说明
 * @author: huangshimin
 * @date: 2021/8/9 11:25 下午
 */
public class WatermarkerFeature {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> ds = env.fromElements(1L, 2L);
        ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Long>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event));

        // 如果存在空闲subtask，1.11之前watermark由于watermark计算机制取所有subtask的最小watermark，空闲subtask会导致其他subtask数据无法正常
        // 触发，使用withIdleness标识过一段时间没有数据来则标识为空闲数据源
        ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Long>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event)
                .withIdleness(Duration.ofSeconds(10)));

        // 使用kafka自身数据的时间戳
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("myTopic", new SimpleStringSchema(),
                new Properties());
        kafkaSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));

    }
}
