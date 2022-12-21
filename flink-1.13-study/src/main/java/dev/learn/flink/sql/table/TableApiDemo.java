package dev.learn.flink.sql.table;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @fileName: TableApiDemo.java
 * @description: table api demo
 * @author: huangshimin
 * @date: 2022/12/20 7:04 PM
 */
public class TableApiDemo {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build());

        tableEnv.createTemporaryTable("tempSourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());

        // 创建blackhole sink
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE tempSourceTable " +
                "(EXCLUDING OPTIONS) ");

        // insert table
//        tableEnv.from("tempSourceTable").insertInto("SinkTable").execute();


        // 创建虚拟表
        tableEnv.createTemporaryView("tempSourceView", tableEnv.from("tempSourceTable"));

        // register udf
//        tableEnv.createFunction("split_str", );

        // table api test
        Table table = tableEnv.from("tempSourceTable")
                .filter($("id").isEqual(10))
                .groupBy($("id"))
                .select($("id"), $("name").count().as("nameCount"));
        // sql
        tableEnv.sqlQuery("select id,count(name) from tempSourceTable where id =10 group by id").execute().print();

        // statementSet
        tableEnv.createStatementSet()
                .addInsertSql("insert into a select * from b")
                .addInsertSql("insert into c select * from b")
                .execute();

    }
}
