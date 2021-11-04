package dev.learn.flink.hudi;

import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @fileName: CDC2Hudi.java
 * @description: flink cdc sink hudi
 * @author: huangshimin
 * @date: 2021/10/30 8:07 下午
 */
public class CDC2Hudi {
    private static String getCDCMysqlSource() {
        return "create table order_source(" +
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
                "'server-id'='101'," +
                "'table-name'='orders')";
    }

    private static String getHudiSink() {
        return "create table order_sink(" +
                "order_number int," +
                "order_date string," +
                "purchaser int," +
                "quantity int," +
                "product_id int)" +
                "PARTITIONED BY (order_number)with(" +
                "'connector'='hudi'," +
                "'path'='hdfs:///user/hudi/order'," +
                "'hoodie.datasource.write.recordkey.field'='order_number'," +
                "'write.precombine.field'='order_date')";
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofSeconds(3));
        tableEnv.executeSql(getCDCMysqlSource());
        tableEnv.executeSql(getHudiSink());

//        tableEnv.executeSql("insert into order_sink select * from order_source");
        tableEnv.executeSql("select * from order_source").print();
    }
}
