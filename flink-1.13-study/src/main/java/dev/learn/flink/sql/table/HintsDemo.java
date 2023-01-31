package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: JoinHintsDemo.java
 * @description: flink hints demo
 * @author: huangshimin
 * @date: 2023/1/31 10:44
 */
public class HintsDemo {


    /**
     * join hints
     *
     * @param tableEnvironment
     */
    public static void joinHints(StreamTableEnvironment tableEnvironment) throws Exception {
        Table leftTable = tableEnvironment.fromValues(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING())), row(1, "zs"), row(2L, "ls"));


        Table rightTable = tableEnvironment.fromValues(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("work", DataTypes.STRING())), row(1, "c++ engineer"), row(2L, "java engineer"));

        tableEnvironment.createTemporaryView("t1", leftTable);
        tableEnvironment.createTemporaryView("t2", rightTable);

        tableEnvironment.executeSql("select /*+ BROADCAST(t1) */ t1.id,t1.name,t2.work from t1 left join t2 on t1.id "
                + "=t2.id").print();
    }

    public static void queryHints(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("create table kafkaSource(" +
                "id int,name string)with(  'connector' = 'kafka',\n" +
                "  'topic' = 'test_query_hints',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv')");
        tableEnvironment.executeSql("select  * from kafkaSource /*+ OPTIONS('scan.startup.mode'='latest-offset') */")
                .print();

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamEnv,
                EnvironmentSettings.newInstance().inStreamingMode().build());
        // join hints
//        joinHints(streamTableEnvironment);
        // query hints
        queryHints(streamTableEnvironment);
//        streamEnv.execute();
    }

}
