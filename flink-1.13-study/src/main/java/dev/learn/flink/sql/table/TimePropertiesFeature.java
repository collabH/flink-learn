package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @fileName: TimePropertiesFeature.java
 * @description: Table时间属性
 * @author: huangshimin
 * @date: 2021/8/29 4:49 下午
 */
public class TimePropertiesFeature {
    static String processTimeSql = "create table data_proc(" +
            "name string," +
            "age int," +
            "proc_time as PROCTIME())WITH(" +
            "'connector'='datagen')";

    static String eventTimeSql = "create table data_event(" +
            "name string," +
            "age int," +
            "event_time timestamp_ltz(3)," +
            "WATERMARK for event_time as event_time - interval '5' second)WITH(" +
            "'connector'='datagen')";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        eventTimeTable(tableEnvironment);
    }

    /**
     * process time
     *
     * @param tableEnvironment
     */
    private static void processTime(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(processTimeSql);
        String querySql = "select tumble_end(proc_time,INTERVAL '10' SECOND) as window_end,count(distinct name) as " +
                "name_count from data_proc group by tumble(proc_time,INTERVAL '10' SECOND)";
        tableEnvironment.executeSql(querySql)
                .print();
    }

    /**
     * process time table
     *
     * @param tableEnvironment
     */
    private static void processTimeTable(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(processTimeSql);
        Table table = tableEnvironment.from("data_proc");
        GroupWindowedTable window = table
                .window(Tumble.over(lit(10).second())
                        .on($("proc_time"))
                        .as("window"));
        window.groupBy($("window"))
                .select($("window").end().as("end_time"), $("name").count().distinct().as("name_count")).execute().print();
    }

    /**
     * eventtime sql
     *
     * @param tableEnvironment
     */
    private static void eventTime(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(eventTimeSql);
        String querySql = "select tumble_start(event_time,interval '10' second) as window_start,count(distinct name) " +
                "as " +
                "name_count from data_event group by tumble(event_time,interval '10' second)";
        tableEnvironment.executeSql(querySql).print();
    }

    /**
     * eventtime sql
     *
     * @param tableEnvironment
     */
    private static void eventTimeTable(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(eventTimeSql);
        Table table = tableEnvironment.from("data_event");
        table.window(Tumble.over(lit(10).second())
                .on($("event_time"))
                .as("window")).groupBy($("window"))
                .select($("window").start().as("window_start"), $("name")
                        .count().distinct().as("name_count"))
                .execute().print();
    }


}
