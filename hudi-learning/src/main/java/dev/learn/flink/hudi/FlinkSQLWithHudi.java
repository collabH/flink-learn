package dev.learn.flink.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: FlinkSQLWithHudi.java
 * @description: hudi on flinksql
 * @author: huangshimin
 * @date: 2021/10/28 7:26 下午
 */
public class FlinkSQLWithHudi {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment env =
                StreamTableEnvironment.create(streamEnv,
                        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        // create hudi table
        // streaming read
        env.executeSql("create table order_sink(" +
                "order_number int," +
                "order_date string," +
                "purchaser int," +
                "quantity int," +
                "product_id int)" +
                "PARTITIONED BY (order_number)with(" +
                "'connector'='hudi'," +
                "'path'='hdfs:///user/hudi/order'," +
                "'hoodie.datasource.write.recordkey.field'='order_number'," +
                " 'table.type' = 'MERGE_ON_READ'," +
                " 'read.streaming.enabled' = 'true'," +
                " 'read.streaming.check-interval' = '4'," +
                "'write.precombine.field'='order_date')");



//        env.executeSql("insert into hudi_table values(1,'hsm')");
//        env.executeSql("insert into hudi_table values(1,'ls')");
        env.executeSql("select * from  order_sink").print();
//        env.executeSql("select count(*) from  hudi_table").print();
    }
}
