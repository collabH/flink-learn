package dev.learn.flink.tablesql.hive;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @fileName: UseHiveCatalog.java
 * @description: UseHiveCatalog.java类说明
 * @author: by echo huang
 * @date: 2020/9/6 6:19 下午
 */
public class UseHiveCatalog {
    public static void main(String[] args) throws DatabaseNotExistException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

        String hiveCatalogName = "myhive";
        String databaseName = "bussine_dws";
        String hiveConfDir = "/Users/babywang/Documents/reserch/studySummary/module/hive/apache-hive-2.3.6-bin/conf";
        String hiveVersion = "2.3.5";
        HiveCatalog hiveCatalog = new HiveCatalog(hiveCatalogName, databaseName, hiveConfDir, hiveVersion);
        tableEnv.registerCatalog(hiveCatalogName, hiveCatalog);

        // 设置dynamic table config
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");

        tableEnv.useCatalog(hiveCatalogName);
        tableEnv.useDatabase(databaseName);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        // 流式读取
        tableEnv.sqlQuery("select * from dws_user_action")
                .executeInsert("dws_user");
    }
}
