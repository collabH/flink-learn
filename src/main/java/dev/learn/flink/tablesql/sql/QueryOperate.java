package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

/**
 * @fileName: QueryOperate.java
 * @description: QueryOperate.java类说明
 * @author: by echo huang
 * @date: 2021/1/14 9:40 下午
 */
public class QueryOperate {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamEnvironment.getEnv(executionEnvironment);
        String catalogName = "hive";
        String databaseName = "for_ods";
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, databaseName, "/Users/babywang/Documents/reserch/studySummary/module/hive/apache-hive-2.3.6-bin/conf");

        env.registerCatalog(catalogName, hiveCatalog);

        env.useCatalog(catalogName);
        env.useDatabase(databaseName);
        env.getConfig().setSqlDialect(SqlDialect.HIVE);
        Table options = env.from("hive_sink_for_os_questionnaire_options1").where($("questionnaire_id").isNotNull());
        Table lists = env.from("hive_sink_for_os_questionnaire_lists1")
                .where($("questionnaire_id").isNotNull());

        env.sqlQuery("select * from " + options + " as options left join " + lists + " as lists on options.questionnaire_id=lists.questionnaire_id").limit(1).execute().print();

        env.executeSql("create external table if not exists test_ext(id int,name string) stored as parquet");
        env.executeSql("insert into test_ext values(1,'hsm')");

        Table test_ext = env.from("test_ext");

        test_ext.printSchema();
        test_ext.execute().print();
        // group by
        env.executeSql("select count(id) as cn from test_ext group by id").print();

        // over window
//        env.executeSql("select rn from(select row_number()over(partition by id order by id) as rn from test_ext)").print();

        // drop operate
        env.executeSql("drop table test_ext");
        env.executeSql("drop database for_ods");
    }
}
