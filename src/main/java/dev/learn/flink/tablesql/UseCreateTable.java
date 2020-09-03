package dev.learn.flink.tablesql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @fileName: UseCreateTable
 * @description: use create table
 * @author: by echo huang
 * @date: 2020/9/3 11:20 上午
 */
public class UseCreateTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        DataStreamSource<Integer> datasource = env.fromElements(1, 2, 3, 4, 5);
        tableEnvironment.createTemporaryView("test_temp_view", datasource);


        Table table = tableEnvironment.fromDataStream(datasource, $("id"));

        table = table.select($("id"))
                .filter($("id").isGreater(2));

        // append流
        tableEnvironment.toAppendStream(table, Row.class)
                .print();
        // retract流
        tableEnvironment.toRetractStream(table, Row.class)
                .print();

        // sql方式

        tableEnvironment.createTemporaryView("test", datasource);

        tableEnvironment.sqlQuery("select * from test")
                .execute().print();


        // 查看执行计划
        String explain = table.explain();
//        System.out.println(explain);

        env.execute();
    }
}
