package dev.learn.flink.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: FlinkCDCSourceTest.java
 * @description: FlinkCDCSourceTest.java类说明
 * @author: by echo huang
 * @date: 2020/11/2 5:43 下午
 */
public class FlinkCDCSourceTest {
    public static void main(String[] args) throws Exception {
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop")
                .port(3306)
                .databaseList("ds1")
                .username("root")
                .password("root")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print().setParallelism(1);

        env.execute();
    }
}
