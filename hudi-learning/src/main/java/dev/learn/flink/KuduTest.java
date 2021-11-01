package dev.learn.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: KuduTest.java
 * @description: KuduTest.java类说明
 * @author: by echo huang
 * @date: 2021/1/31 7:13 下午
 */
public class KuduTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build());
//        KuduCatalog catalog = new KuduCatalog("kudu", "hadoop:7051");

//        tableEnv.registerCatalog("kudu", catalog);
        tableEnv.useCatalog("kudu");
        tableEnv.executeSql("drop table ods_user");
        tableEnv.executeSql("create table ods_user(id bigint,name string,sex int)with('kudu.hash-columns'='id','kudu.primary-key-columns'='id')");
        tableEnv.useCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG);
        tableEnv.executeSql("create table kafka_stream_user(id bigint,name string,sex int,primary key(id) not enforced)with('connector'='kafka',\n" +
                "'topic'='test.ds1.user',\n" +
                "'properties.bootstrap.servers'='hadoop:9092,hadoop:9093,hadoop:9094',\n" +
                "'properties.group.id'='testGroup',\n" +
                "'scan.startup.mode'='earliest-offset',\n" +
                "'format'='debezium-json')");

        tableEnv.executeSql("insert into kudu.default_database.ods_user select * from default_catalog.default_database.kafka_stream_user");


    }
}
