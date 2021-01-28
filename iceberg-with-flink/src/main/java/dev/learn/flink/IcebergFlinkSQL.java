package dev.learn.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.ExecutionException;

/**
 * @fileName: IcebergWithFlink.java
 * @description: IcebergWithFlink.java类说明
 * @author: by echo huang
 * @date: 2021/1/27 11:43 下午
 */
public class IcebergFlinkSQL {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build());
        tableEnv.executeSql("CREATE CATALOG hive_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://localhost:9083',\n" +
                "  'clients'='5',\n" +
                "  'property-version'='1',\n" +
                "  'warehouse'='hdfs://hadoop:8020/user/hive/warehouse'\n" +
                ")");
        tableEnv.useCatalog("hive_catalog");
        tableEnv.executeSql("create database if not exists iceberg_db");
        tableEnv.useDatabase("iceberg_db");
//        tableEnv.executeSql("create table test_iceberg(id int,name string)with(" +
//                "'write.format.default'='parquet')");
        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("insert overwrite test_iceberg values(1,'hsm')");
//        statementSet.addInsertSql("insert into test_iceberg values(2,'hsm1')");
        statementSet.execute().getJobClient().get().getJobExecutionResult(IcebergFlinkSQL.class.getClassLoader())
                .get();
        tableEnv.executeSql("select * from test_iceberg").print();
    }
}
