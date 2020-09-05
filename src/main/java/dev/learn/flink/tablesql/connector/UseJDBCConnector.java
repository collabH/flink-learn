package dev.learn.flink.tablesql.connector;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: UseJDBCConnector.java
 * @description: 使用JBDC Connector
 * @author: by echo huang
 * @date: 2020/9/5 6:07 下午
 */
public class UseJDBCConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

//        SingleOutputStreamOperator<MessageEvent> eventStream = env.socketTextStream("hadoop", 9888)
//                .map(data -> {
//                    String[] arr = data.split(",");
//                    return new MessageEvent(arr[0], arr[1], arr[2]);
//                }).returns(Types.POJO(MessageEvent.class));

        String ddl = "create table event(" +
                "id string,\n" +
                "`timestamp` string,\n" +
                "temperature string,\n" +
                " PRIMARY KEY (id) NOT ENFORCED)with(" +
                "'connector'='jdbc',\n" +
                "'url'='jdbc:mysql://localhost:3306/ds1',\n" +
                "'username'='root',\n" +
                "'password'='root',\n" +
                "'table-name'='event')";
        tableEnv.executeSql(ddl);

        tableEnv.from("event")
                .execute()
                .print();

//        Table table = tableEnv.fromDataStream(eventStream, $("id"), $("timestamp"), $("temperature"));

//        table.executeInsert("event");
//        env.execute();
    }
}
