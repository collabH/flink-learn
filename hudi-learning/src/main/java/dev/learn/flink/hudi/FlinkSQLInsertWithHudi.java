package dev.learn.flink.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: FlinkSQLReadWithHudi.java
 * @description: FlinkSQLReadWithHudi.java类说明
 * @author: huangshimin
 * @date: 2021/10/29 11:15 上午
 */
public class FlinkSQLInsertWithHudi {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment env =
                StreamTableEnvironment.create(streamEnv,
                        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        // create hudi table
        // streaming read
//        env.executeSql("create table hudi_table(" +
//                "id int," +
//                "name string)PARTITIONED BY (id)with(" +
//                " 'connector' = 'hudi'," +
//                " 'path'='file:///Users/huangshimin/Documents/study/hudi'," +
//                " 'hoodie.datasource.write.recordkey.field'='id'," +
//                " 'write.precombine.field'='name')");

//        env.executeSql(bulkInsertSql());
        env.executeSql(insertDataToHdfs());
        env.executeSql("insert into hudi_table values(1,'hsm'),(2,'wy'),(3,'ls'),(4,'ww'),(5,'zl')");
    }

    private static String bulkInsertSql() {
        return "create table hudi_table(" +
                "id int," +
                "name string)PARTITIONED BY (id)with(" +
                " 'connector' = 'hudi'," +
                " 'path'='file:///Users/huangshimin/Documents/study/hudi'," +
                " 'hoodie.datasource.write.recordkey.field'='id'," +
                " 'write.operation'='bulk_insert'," +
                " 'write.precombine.field'='name')";
    }
    
    private static String insertDataToHdfs(){
        return "create table hudi_table(" +
                "id int," +
                "name string)PARTITIONED BY (id)with(" +
                " 'connector' = 'hudi'," +
                " 'path'='hdfs:///Users/huangshimin/Documents/study/hudi'," +
                " 'hoodie.datasource.write.recordkey.field'='id'," +
                " 'write.precombine.field'='name')";
    }
}
