package dev.learn.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * @fileName: PulsarDataStreamDemo.java
 * @description: pulsar ds案例
 * @author: huangshimin
 * @date: 2023/2/2 11:53
 */
public class PulsarDataStreamDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        String serviceUrl = "";
        String adminUrl = "";
        PulsarSource<String> source = PulsarSource.builder()
                .setServiceUrl(serviceUrl)
                .setAdminUrl(adminUrl)
                .setStartCursor(StartCursor.earliest())
                .setTopics("my-topic")
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName("my-subscription")
                .setSubscriptionType(SubscriptionType.Key_Shared)
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
    }
}
