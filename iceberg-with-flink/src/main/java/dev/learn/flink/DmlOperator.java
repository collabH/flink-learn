package dev.learn.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @fileName: QuerySqlOperator.java
 * @description: QuerySqlOperator.java类说明
 * @author: huangshimin
 * @date: 2021/7/31 7:15 下午
 */
public class DmlOperator {
    public static void main(String[] args) {
        TableEnvironment tableEnv = FlinkEnv.getTableEnv();
        // catalog
        String hiveCatalog = "create catalog hive_catalog with(" +
                "'type'='iceberg'," +
                "'catalog-type'='hive'," +
                "'clients'='5'," +
                "'property-version'='1'," +
                "'warehouse'='hdfs://localhost:8020/user/hive/warehouse')";

        tableEnv.executeSql(hiveCatalog);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration
                .set(ConfigOptions.key("execution.type")
                        .stringType()
                        .noDefaultValue(), "streaming");

        tableEnv.useCatalog("hive_catalog");
        tableEnv.useDatabase("flink_iceberg");
        // batch read
        String selectSql = "select * from test";

        // streaming read
        // SET table.dynamic-table-options.enabled=true; 开启动态参数
        String selectSqlStreaming = "SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', " +
                "'start-snapshot-id'='3821550127947089987')*/ ";

        tableEnv.sqlQuery(selectSql)
                .execute().print();
        // Insert Sql
        String insertSql = "INSERT INTO hive_catalog.default.sample VALUES (1, 'a')";
        String insertSelectSql = "INSERT INTO hive_catalog.default.sample SELECT id, data from other_kafka_table";

        String insertOverwriteSql="INSERT OVERWRITE sample VALUES (1, 'a')";
        tableEnv.executeSql(insertSql);
    }
}
