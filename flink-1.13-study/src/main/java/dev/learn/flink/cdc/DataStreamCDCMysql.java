package dev.learn.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

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

        MySqlSource<String> cdcMysqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                // tableList添加新表时不影响历史表
                .scanNewlyAddedTableEnabled(true)
                .databaseList("test_flink")
                .tableList("users")
                .username("cdc_user")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .heartbeatInterval(Duration.ofSeconds(10))
                .build();
        streamEnv.fromSource(cdcMysqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .print().setParallelism(1);
        streamEnv.execute();
    }
}
