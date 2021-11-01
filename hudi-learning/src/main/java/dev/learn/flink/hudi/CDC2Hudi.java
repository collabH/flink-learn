package dev.learn.flink.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: CDC2Hudi.java
 * @description: flink cdc sink hudi
 * @author: huangshimin
 * @date: 2021/10/30 8:07 下午
 */
public class CDC2Hudi {
    private static String getCDCMysqlSource() {
        return "create table cdc_source(" +
                "id int," +
                "username string," +
                "age int," +
                "primary key(id) not enforced)with(" +
                "'connector'='mysql-cdc'," +
                "'hostname'='hadoop'," +
                "'username'='root'," +
                "'port'='3306'," +
                "'password'='root'," +
                "'database-name'='cdc'," +
                "'table-name'='user')";
    }

    private static String getHudiSink() {
        return "create table hudi_sink(" +
                "id int," +
                "username string," +
                "age int)PARTITIONED BY (id)with(" +
                "'connector'='hudi'," +
                "'path'='hdfs:///user/hudi/user'," +
                "'hoodie.datasource.write.recordkey.field'='id'," +
                "'write.precombine.field'='username')";
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        tableEnv.executeSql(getCDCMysqlSource());
        tableEnv.executeSql(getHudiSink());

        tableEnv.executeSql("insert into hudi_sink select * from cdc_source").print();
    }
}
