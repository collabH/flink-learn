package dev.learn.flink.sql.grammar;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: InsertSqlFeature.java
 * @description: InsertSqlFeature.java类说明
 * @author: huangshimin
 * @date: 2021/9/2 9:27 下午
 */
public class InsertSqlFeature {
    public static void main(String[] args) {
        String ddl = "create table test(id int,name string)with( 'connector' = 'print')";
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamEnv);
        streamTableEnvironment.executeSql(ddl);
        streamTableEnvironment.executeSql("insert into test values(1,'hsm')").print();
    }
}
