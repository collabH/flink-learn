package dev.learn.flink.cep;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
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

import static dev.learn.flink.cep.UserVisitStatus.CREATE_ORDER;
import static dev.learn.flink.cep.UserVisitStatus.PAY;
import static dev.learn.flink.cep.UserVisitStatus.SUBMIT_ORDER;

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
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "cep_client_id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 4);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("cep_test", new SimpleStringSchema(), props);
        KeyedStream<User, String> users = environment.addSource(consumer)
                .map(value -> {
                    System.out.println(value);
                    return JSON.parseObject(value, User.class);
                })
                .returns(TypeInformation.of(User.class))
                .keyBy(new KeySelector<User, String>() {
                    @Override
                    public String getKey(User value) throws Exception {
                        return value.getUserId();
                    }
                });

        // 计算超时订单
        Pattern<User, User> timeoutOrder = Pattern.<User>begin("create_order")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User value) throws Exception {
                        return CREATE_ORDER.getStatus() == value.getStatus();
                    }
                }).next("submit_order")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User value) throws Exception {
                        return SUBMIT_ORDER.getStatus() == value.getStatus();
                    }
                }).next("pay")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User value) throws Exception {
                        return PAY.getStatus() == value.getStatus();
                    }
                    // 30s超时账单
                }).within(Time.seconds(30));
        OutputTag<String> timeoutOutputTag = new OutputTag<>("timeoutOrder",TypeInformation.of(String.class));
        SingleOutputStreamOperator<String> result = CEP.pattern(users, timeoutOrder)
                .flatSelect(timeoutOutputTag,
                        new PatternFlatTimeoutFunction<User, String>() {
                            @Override
                            public void timeout(Map<String, List<User>> map, long l, Collector<String> collector) throws Exception {
                                map.forEach((key, value) -> {
                                    for (User user : value) {
                                        collector.collect("userId:" + key + " status:" + user.getStatus() + "订单支付超时!");
                                    }
                                });
                            }
                        }, new PatternFlatSelectFunction<User, String>() {
                            @Override
                            public void flatSelect(Map<String, List<User>> map, Collector<String> collector) throws Exception {
                                map.forEach((key, value) -> {
                                    for (User user : value) {
                                        collector.collect("userId:" + key + " status:" + user.getStatus() + "订单支付成功，等待发货!");
                                    }
                                });
                            }
                        });

        result.print();

        result.getSideOutput(timeoutOutputTag).print();

        environment.execute();
    }
}

@Data
class User {
    private String userId;
    private Integer status;
}


@AllArgsConstructor
enum UserVisitStatus {
    CREATE_ORDER(1),
    SUBMIT_ORDER(2),
    PAY(3),
    FINISHED(4);

    @Getter
    private final int status;
}