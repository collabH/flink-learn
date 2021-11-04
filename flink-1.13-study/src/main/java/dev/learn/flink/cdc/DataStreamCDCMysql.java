package dev.learn.flink.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: DataStreamCDCMysql.java
 * @description: DataStreamCDCMysql.java类说明
 * @author: huangshimin
 * @date: 2021/11/4 6:44 下午
 */
public class DataStreamCDCMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        streamEnv.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        DebeziumSourceFunction<String> cdcMysqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("inventory")
                .tableList("orders", "geom", "products", "products_on_hand", "customers", "addresses")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .build();
        streamEnv.addSource(cdcMysqlSource, "MySQL CDC Source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .print().setParallelism(1);
        streamEnv.execute();
    }
}
