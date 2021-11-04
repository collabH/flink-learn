package dev.learn.flink.cdc;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @fileName: CDCMysql.java
 * @description: CDCMysql.java类说明
 * @author: huangshimin
 * @date: 2021/11/4 3:37 下午
 */
public class FlinkSQLCDCMysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofSeconds(3));
        tableEnv.executeSql("create table orders(" +
                "order_number int," +
                "order_date string," +
                "purchaser int," +
                "quantity int," +
                "product_id int," +
                "primary key(order_number) not enforced)with(" +
                "'connector'='mysql-cdc'," +
                "'hostname'='localhost'," +
                "'username'='root'," +
                "'port'='3306'," +
                "'password'='123456'," +
                "'database-name'='inventory'," +
                "'server-time-zone'='Asia/Shanghai'," +
                "'table-name'='orders')");
        tableEnv.executeSql("SELECT * FROM orders").print();
    }
}
