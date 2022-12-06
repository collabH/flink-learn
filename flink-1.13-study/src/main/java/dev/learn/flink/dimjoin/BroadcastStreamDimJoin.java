package dev.learn.flink.dimjoin;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @fileName: BroadcastStreamDimJoin.java
 * @description: 广播流dim join
 * @author: huangshimin
 * @date: 2022/12/6 4:35 PM
 */
public class BroadcastStreamDimJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        KafkaSource<String> primaryKeySource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9091")
                .setClientIdPrefix("source")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("source_topic")
                .setUnbounded(OffsetsInitializer.earliest())
                .setGroupId("source_group_id")
                .build();

        KafkaSource<String> dimSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9091")
                .setClientIdPrefix("broadcast")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("broadcast_topic")
                .setUnbounded(OffsetsInitializer.earliest())
                .setGroupId("broadcast_group_id")
                .build();
        // 定义广播MapStateDescriptor
        MapStateDescriptor<Integer, String> mapStateDescriptor = new MapStateDescriptor<>("broadcastState",
                TypeInformation.of(Integer.class), TypeInformation.of(String.class));

        BroadcastStream<Tuple2<Integer, String>> broadcastSource = env.fromSource(dimSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                        "broadcastSource")
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple2.of(Integer.parseInt(split[0]), split[1]);
                    }
                }).name("map_func").uid("map_uid")
                .broadcast(mapStateDescriptor);
        // 获取dimJoinSource
        BroadcastConnectedStream<String, Tuple2<Integer, String>> dimJoinSource = env.fromSource(primaryKeySource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                        "primaryKeySource")
                .connect(broadcastSource);
        // process dim join
        dimJoinSource.process(new BroadcastProcessFunction<String, Tuple2<Integer, String>, String>() {
            @Override
            public void processElement(String key,
                                       BroadcastProcessFunction<String, Tuple2<Integer, String>, String>.ReadOnlyContext readOnlyContext,
                                       Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<Integer, String> broadcastState =
                        readOnlyContext.getBroadcastState(mapStateDescriptor);
                // 从广播状态中根据key查询对应dim数据
                String value = broadcastState.get(Integer.parseInt(key));
                // 输出结果
                if (StringUtils.isNotEmpty(value)) {
                    collector.collect(key + ":" + value);
                }
            }

            @Override
            public void processBroadcastElement(Tuple2<Integer, String> dimSource,
                                                BroadcastProcessFunction<String, Tuple2<Integer, String>, String>.Context context,
                                                Collector<String> collector) throws Exception {
                // 将广播流数据放入广播状态中
                BroadcastState<Integer, String> broadcastState = context.getBroadcastState(mapStateDescriptor);
                broadcastState.put(dimSource.f0, dimSource.f1);
            }
        }).print("data");
        env.execute();
    }
}
