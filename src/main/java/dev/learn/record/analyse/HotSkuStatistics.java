package dev.learn.record.analyse;

import com.alibaba.fastjson.JSON;
import dev.learn.record.domain.UserBehavior;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @fileName: HotSkuStastic.java
 * @description: 热点商品pv统计
 * @author: by echo huang
 * @date: 2020/9/6 9:06 下午
 */
public class HotSkuStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60 * 10 * 1000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        checkpointConfig.enableUnalignedCheckpoints(true);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000 * 60);
        checkpointConfig.setCheckpointTimeout(15 * 60 * 1000);
        checkpointConfig.setTolerableCheckpointFailureNumber(2);
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000));

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user_behavior_group1");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024 * 5);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop:9092,hadoop:9093,hadoop:9094");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        FlinkKafkaConsumer<UserBehavior> cosumerSource = new FlinkKafkaConsumer<>("user_behavior", new KafkaDeserializationSchema<UserBehavior>() {
            @Override
            public boolean isEndOfStream(UserBehavior userBehavior) {
                return Objects.isNull(userBehavior);
            }

            @Override
            public UserBehavior deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                return JSON.parseObject(new String(consumerRecord.value(), Charset.defaultCharset()), UserBehavior.class);
            }

            @Override
            public TypeInformation<UserBehavior> getProducedType() {
                return TypeInformation.of(UserBehavior.class);
            }
        }, props);

        FlinkKafkaConsumerBase<UserBehavior> userBehaviorFlinkKafkaConsumerBase = cosumerSource.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<UserBehavior>() {
            @Override
            public WatermarkGenerator<UserBehavior> createWatermarkGenerator(Context context) {
                return new WaterMark(Duration.ofSeconds(30));
            }
        }));

        env.addSource(userBehaviorFlinkKafkaConsumerBase)
                .keyBy(new KeySelector<UserBehavior, Long>() {
                    @Override
                    public Long getKey(UserBehavior value) throws Exception {
                        return value.getItemId();
                    }
                }).timeWindow(Time.minutes(5), Time.seconds(30))
                .process(new PvProcessFunction())
                .print();

        env.execute();

    }
}

class WaterMark implements WatermarkGenerator<UserBehavior> {

    /**
     * The maximum timestamp encountered so far.
     */
    private long maxTimestamp;

    /**
     * The maximum out-of-orderness that this watermark generator assumes.
     */
    private final long outOfOrdernessMillis;

    /**
     * Creates a new watermark generator with the given out-of-orderness bound.
     *
     * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
     */
    public WaterMark(Duration maxOutOfOrderness) {
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    @Override
    public void onEvent(UserBehavior event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, event.getTimestamp());
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }
}

/**
 * pv统计函数
 */
class PvProcessFunction extends ProcessWindowFunction<UserBehavior, Tuple4<Long, Long, Long, Long>, Long, TimeWindow> {

    @Override
    public void process(Long key, Context context, Iterable<UserBehavior> elements, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
        TimeWindow window = context.window();
        long end = window.getEnd();
        long start = window.getStart();
        long pv = 0L;
        for (UserBehavior ignored : elements) {
            pv += 1;
        }
        out.collect(new Tuple4<>(start, end, key, pv));
    }
}