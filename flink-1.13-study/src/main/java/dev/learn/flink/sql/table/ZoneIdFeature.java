package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @fileName: ZoneIdFeature.java
 * @description: 1.13时区类型
 * @author: huangshimin
 * @date: 2021/8/29 5:30 下午
 */
public class ZoneIdFeature {
    static String timestampLtzSql = "create table ltz(" +
            "name string," +
            "times timestamp_ltz)with(" +
            "'connector'='datagen')";

    static String timestampSql = "create table ltz(" +
            "name string," +
            "times timestamp)with(" +
            "'connector'='datagen')";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
//        timeStamp(tableEnvironment);
        // proctime timestamp_ltz
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tableEnvironment.executeSql("select proctime()").print();
    }

    /**
     * ltz时区
     * @param tableEnvironment
     */
    private static void timeStampLtz(StreamTableEnvironment tableEnvironment) {
//        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Australia/Darwin"));
//        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tableEnvironment.executeSql(timestampLtzSql);
        tableEnvironment.executeSql("select * from ltz").print();
    }



    /**
     * 无时区类型
     * @param tableEnvironment
     */
    private static void timeStamp(StreamTableEnvironment tableEnvironment) {
//        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Australia/Darwin"));
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
//        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tableEnvironment.executeSql(timestampSql);
        tableEnvironment.executeSql("select * from ltz").print();
    }

}
