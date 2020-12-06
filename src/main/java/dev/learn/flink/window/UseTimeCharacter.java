package dev.learn.flink.window;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @fileName: UseTimeCharacter.java
 * @description: 时间语义
 * @author: by echo huang
 * @date: 2020-08-30 19:10
 */
public class UseTimeCharacter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop:9092,hadoop:9093,hadoop:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "watermark-test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("watermark", new SimpleStringSchema(), props);

        env.addSource(consumer)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        return JSON.parseObject(value, Event.class);
                    }
                })
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
                    @Override
                    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new TimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTs();
                            }
                        };
                    }

                    @Override
                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Event>() {
                            private long maxTs;
                            private long delayTs = 5000;

                            @Override
                            public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                                maxTs = Math.max(maxTs, eventTimestamp);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                output.emitWatermark(new Watermark(maxTs - delayTs));
                            }
                        };
                    }
                }).keyBy(new KeySelector<Event, Integer>() {
            @Override
            public Integer getKey(Event value) throws Exception {
                return value.getId();
            }
        })
                .timeWindow(Time.milliseconds(1000))
                .process(new ProcessWindowFunction<Event, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        List<Event> list = new ArrayList<>();
                        for (Event element : elements) {
                            list.add(element);
                        }
                        out.collect(JSON.toJSONString(list));
                    }
                });

        env.execute();
    }
}

@Data
class Event {
    private Integer id;
    private String name;
    private long ts;
}
