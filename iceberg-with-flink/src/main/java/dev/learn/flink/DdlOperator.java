package dev.learn.flink;

import org.apache.flink.table.api.TableEnvironment;

/**
 * @fileName: DdlOperator.java
 * @description: Iceberg DDL operator
 * @author: huangshimin
 * @date: 2021/7/31 3:17 下午
 */
public class DdlOperator {
    public static void main(String[] args) {
        TableEnvironment tableEnv = FlinkEnv.getTableEnv();
        // catalog
        String hiveCatalog = "create catalog hive_catalog with(" +
                "'type'='iceberg'," +
                "'catalog-type'='hive'," +
                "'clients'='5'," +
                "'property-version'='1'," +
                "'warehouse'='hdfs://localhost:8020/user/hive/warehouse')";

        String hadoopCatalog = "create catalog hadoop_catalog with(" +
                "'type'='iceberg'," +
                "'catalog-type'='hadoop'," +
                "'clients'='5'," +
                "'property-version'='1'," +
                "'warehouse'='hdfs://localhost:8020/user/hive/warehouse')";
        tableEnv.executeSql(hiveCatalog);

        // ddl
        tableEnv.useCatalog("hive_catalog");
        // create database
        tableEnv.executeSql("create database flink_iceberg");
        tableEnv.useDatabase("flink_iceberg");

        // create table
        String createTable = "create table if not exists sample(" +
                "id bigint comment 'unique id'," +
                "data string)";
        String createPartitionTable = "create table sample(" +
                "id bigint comment 'unique id'," +
                "data string)partitioned by (data)";

        String createLikeTable = "CREATE TABLE  hive_catalog.default.sample_like LIKE hive_catalog.default.sample";
        tableEnv.executeSql(createTable);

        // alter table
        String alterTableSet = "alter table sample set ('write.format.default'='avro')";
        String alterTableRename = "alter table sample rename to test";


        // drop table
        String dropTable = "drop table test";
    }
}
