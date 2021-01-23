package dev.learn.flink.tablesql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: UseTableEnvironment.java
 * @description: UseTableEnvironment.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 11:25 上午
 */
public class UseTableEnvironment {
    public static void main(String[] args) {
        // create stream execute environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        // create batch execute environment
//        EnvironmentSettings batchSettings = EnvironmentSettings.newInstance()
//                .inBatchMode().useBlinkPlanner().build();
//        TableEnvironment tableEnvironment1 = TableEnvironment.create(batchSettings);


//        Table table = tableEnvironment.fromValues(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT().notNull())), 1, 2, 3, 4, 5);
//
//        tableEnvironment.createTemporaryView("test", table);
//
//        tableEnvironment.executeSql("select * from test").print();
//        tableEnvironment.from("test").execute().print();
//        System.out.println(Arrays.toString(tableEnvironment.listCatalogs()));
//        System.out.println(Arrays.toString(tableEnvironment.listModules()));
//        System.out.println(Arrays.toString(tableEnvironment.listTables()));
//        System.out.println(Arrays.toString(tableEnvironment.listViews()));
//        System.out.println(Arrays.toString(tableEnvironment.listTemporaryTables()));
//        System.out.println(Arrays.toString(tableEnvironment.listTemporaryViews()));
//        System.out.println(tableEnvironment.explainSql("select * from test", ExplainDetail.ESTIMATED_COST));


        // test custom http
        tableEnvironment.executeSql("create table testhttp(id string,name string)with(" +
                "'http.client.request-url'='http://hadoop:8080/test'," +
                "'http.client.format.classname'='dev.learn.flink.User'," +
                "'connector'='http'," +
                "'format'='http-json-bean')");


        tableEnvironment.executeSql("CREATE TABLE print_table WITH ('connector' = 'print') LIKE testhttp (EXCLUDING ALL)");

        tableEnvironment.executeSql("insert into print_table select * from testhttp").print();

    }
}
