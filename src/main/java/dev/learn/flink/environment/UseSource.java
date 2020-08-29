package dev.learn.flink.environment;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECEIVE_BUFFER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * @fileName: UseSource.java
 * @description: UseSource.java类说明
 * @author: by echo huang
 * @date: 2020-08-29 17:42
 */
public class UseSource {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合中获取数据
        DataStreamSource<Integer> datasource = env.fromCollection(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));

        // 从元素中获取
        DataStreamSource<Integer> datasource1 = env.fromElements(1, 2, 3, 4);

        // 从socket中获取
        DataStreamSource<String> datasource2 = env.socketTextStream("hadoop", 9999);

        // 从文件中读取数据
        DataStreamSource<String> datasource3 = env.readTextFile("/xxx");

        // 从kafka中读取数据

        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(GROUP_ID_CONFIG, "test-group-id");
        props.put(MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024);
        // 300ms没到1000条记录就进行一次poll
        props.put(MAX_POLL_INTERVAL_MS_CONFIG, 300);
        props.put(SEND_BUFFER_CONFIG, 1024 * 1024 * 5);
        props.put(RECEIVE_BUFFER_CONFIG, 1024 * 1024 * 5);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(CLIENT_ID_CONFIG, "test-client-id");
        props.put(AUTO_OFFSET_RESET_CONFIG, "latest");

        // 设置eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props);

        DataStreamSource<String> datasource4 = env.addSource(consumer);

        // 添加watemark
//        datasource4.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        datasource4.setParallelism(3);

    }
}
