package dev.learn.flink.tablesql.table;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: UseFromOperator.java
 * @description: UseFromOperator.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 10:07 下午
 */
@Slf4j
public class UseScanOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

        Catalog catalog = new GenericInMemoryCatalog("dev", "dev-db");
        tableEnv.registerCatalog("dev", catalog);

        tableEnv.useCatalog("dev");
        tableEnv.useDatabase("dev-db");



        Table table = tableEnv.fromValues(ROW(FIELD("id", BIGINT().notNull()), FIELD("name", VARCHAR(32).notNull())),
                row(1L, "hsm")
                , row(2L, "zhangsan"));

        tableEnv.createTemporaryView("User", table);

//        env.sqlQuery("select id,name from user_table").execute()
//                .print();

        // from
        Table user = tableEnv.from("User");
        // select
        user.select($("id"))
                .execute()
                .print();

        // as
        user.as("id1", "name1")
                .select($("*"))
                .execute()
                .print();

        // where/filter
        user.where($("id").in(1, 2))
                .filter($("name").isEqual("hsm"))
                .execute().print();

//        tableEnv.toAppendStream(table, Row.class).print();

//        tableEnv.toRetractStream(table, Row.class).print();

//        env.execute();
    }
}
