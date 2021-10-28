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
        env.executeSql("create table hudi_table(" +
                "id int," +
                "name string)PARTITIONED BY (id)with(" +
                " 'connector' = 'hudi'," +
                " 'path'='file:///Users/huangshimin/Documents/study/hudi'," +
                " 'hoodie.datasource.write.recordkey.field'='id'," +
                " 'write.precombine.field'='name')");

//        env.executeSql("insert into hudi_table values(1,'hsm')");
        env.executeSql("select * from  hudi_table").print();
        env.executeSql("select count(*) from  hudi_table").print();
    }
}
