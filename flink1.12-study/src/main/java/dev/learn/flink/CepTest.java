package dev.learn.flink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @fileName: CepTest.java
 * @description: CepTest.java类说明
 * @author: by echo huang
 * @date: 2021/3/14 2:39 下午
 */
public class CepTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 4);


        environment.getConfig().setAutoWatermarkInterval(200);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("cep_test", new SimpleStringSchema(), props);
        KeyedStream<User, String> users = environment.addSource(consumer)
                .map(value -> {
                    return JSON.parseObject(value, User.class);
                })
                .returns(TypeInformation.of(User.class))
                .assignTimestampsAndWatermarks(new WatermarkStrategy<User>() {
                    @Override
                    public WatermarkGenerator<User> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new BoundedOutOfOrdernessGenerator();
                    }
                })
                .keyBy(new KeySelector<User, String>() {
                    @Override
                    public String getKey(User value) throws Exception {
                        return value.getUserId();
                    }
                });

        users.print();
        // 计算超时订单
        Pattern<User, User> timeoutOrder = Pattern.<User>begin("create_order")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User value) throws Exception {
                        return UserVisitStatus.CREATE_ORDER.getStatus() == value.getStatus();
                    }
                }).next("submit_order")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User value) throws Exception {
                        return UserVisitStatus.SUBMIT_ORDER.getStatus() == value.getStatus();
                    }
                }).next("pay")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User value) throws Exception {
                        return UserVisitStatus.PAY.getStatus() == value.getStatus();
                    }
                    // 10s超时账单
                }).within(Time.seconds(10));
        OutputTag<String> timeoutOutputTag = new OutputTag<>("timeoutOrder", TypeInformation.of(String.class));
        SingleOutputStreamOperator<String> result = CEP.pattern(users, timeoutOrder)
                .flatSelect(timeoutOutputTag,
                        new PatternFlatTimeoutFunction<User, String>() {
                            @Override
                            public void timeout(Map<String, List<User>> map, long timeoutTime, Collector<String> collector) throws Exception {
                                map.forEach((key, value) -> {
                                    for (User user : value) {
                                        collector.collect("userId:" + user.getUserId() +"ts: "+user.getTs()+ " status:" + user.getStatus() + "订单支付超时:" + timeoutTime + "!");
                                    }
                                });
                            }
                        }, new PatternFlatSelectFunction<User, String>() {
                            @Override
                            public void flatSelect(Map<String, List<User>> map, Collector<String> collector) throws Exception {
                                map.forEach((key, value) -> {
                                    for (User user : value) {
                                        collector.collect("userId:" + user.getUserId() +"ts: "+user.getTs()+ " status:" + user.getStatus() + "订单支付成功，等待发货!");
                                    }
                                });
                            }
                        });

        result.print();

        result.getSideOutput(timeoutOutputTag).print();

        environment.execute();
    }
}

class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<User> {

    private final long maxOutOfOrderness = 3500; // 3.5 秒

    private long currentMaxTimestamp;

    @Override
    public void onEvent(User event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }

}


class User {
    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", status=" + status +
                ", ts=" + ts +
                '}';
    }

    private String userId;
    private Integer status;
    private Long ts;

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}

enum UserVisitStatus {
    CREATE_ORDER(1),
    SUBMIT_ORDER(2),
    PAY(3),
    FINISHED(4);

    private final int status;

    public int getStatus() {
        return status;
    }

    UserVisitStatus(int status) {
        this.status = status;
    }
}