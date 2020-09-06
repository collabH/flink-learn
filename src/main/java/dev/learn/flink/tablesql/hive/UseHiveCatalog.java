package dev.learn.flink.tablesql.hive;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @fileName: UseHiveCatalog.java
 * @description: UseHiveCatalog.java类说明
 * @author: by echo huang
 * @date: 2020/9/6 6:19 下午
 */
public class UseHiveCatalog {
    public static void main(String[] args) throws Exception {
        TableEnvironment tableEnv = StreamEnvironment.getBatchEnv();

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


        // 流式读取
        Table table = tableEnv.sqlQuery("select * from dws_user_action");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // 创建ads层
        tableEnv.executeSql("drop table if exists ads_user");
        tableEnv.executeSql("create table ads_user(total_count bigint)stored as parquet");

        table
                .select($("payment_count").count().as("total_count"))
                .executeInsert("ads_user");


        tableEnv.sqlQuery("select * from ads_user")
                .execute()
                .print();


//        tableEnv.toRetractStream(adsUser, Row.class).print();

//        env.execute();
    }
}
