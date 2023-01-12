package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.and;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @fileName: TableApiDemo.java
 * @description: table api demo
 * @author: huangshimin
 * @date: 2022/12/20 7:04 PM
 */
public class TableApiDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

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
        tableEnv.sqlQuery("select id,count(name) from tempSourceTable where id =10 group by id")
                .execute().print();

        // statementSet
        tableEnv.createStatementSet()
                .addInsertSql("insert into a select * from b")
                .addInsertSql("insert into c select * from b")
                .execute();


        // interval join
        Table left = tableEnv.from("MyTable").select($("a"), $("b"), $("c"), $("ltime"));
        Table right = tableEnv.from("MyTable").select($("d"), $("e"), $("f"), $("rtime"));

        left.join(right, and($("a").isEqual($("d")),
                        // interval join rtime-5 <= ltime < rtime+10,interval 15
                        $("ltime").isGreaterOrEqual($("rtime").minus(lit(5).minutes())),
                        $("ltime").isLess($("rtime").plus(lit(10).minutes()))
                ))
                .select($("a"), $("b"), $("e"), $("ltime"));

        // join udf
        // 注册 User-Defined Table Function
        TableFunction<Tuple3<String,String,String>> split = new TableFunction<Tuple3<String, String, String>>() {
            @Override
            public TypeInformation<Tuple3<String, String, String>> getResultType() {
                return super.getResultType();
            }
        };
        tableEnv.createTemporaryFunction("split", split);

        Table orders = tableEnv.from("Orders");
        Table result = orders
                .joinLateral(call("split", $("c")).as("s", "t", "v"))
                .select($("a"), $("b"), $("s"), $("t"), $("v"));

    }


}
