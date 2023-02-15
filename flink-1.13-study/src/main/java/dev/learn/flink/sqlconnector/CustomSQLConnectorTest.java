package dev.learn.flink.sqlconnector;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: CustomSQLConnectorTest.java
 * @description: CustomSQLConnectorTest.java类说明
 * @author: huangshimin
 * @date: 2023/2/13 20:36
 */
public class CustomSQLConnectorTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv,
                EnvironmentSettings.newInstance().inStreamingMode().build());
        tableEnv.executeSql("create table custom_table(id int,name string)with(" +
                "'connector'='custom'," +
                "'custom.path'='/Users/huangshimin/Documents/study/flink-learn/flink-1.13-study/src/main/resources/data/custom-data.csv'," +
                "'custom.format'='csv')");
        tableEnv.executeSql("select * from custom_table").print();
    }
}
