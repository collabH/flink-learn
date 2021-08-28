package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: TableFeature.java
 * @description: TableFeature.java类说明
 * @author: huangshimin
 * @date: 2021/8/29 12:34 上午
 */
public class TableFeature {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamEnv);
//        changelogStream(streamEnv, tableEnvironment);
        changelogSql(tableEnvironment);
    }

    private static void changelogSql(StreamTableEnvironment tableEnv) throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE GeneratedTable "
                        + "("
                        + "  name STRING,"
                        + "  score INT,"
                        + "  event_time TIMESTAMP_LTZ(3),"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                        + ")"
                        + "WITH ('connector'='datagen')");

        Table table = tableEnv.from("GeneratedTable");


        Table simpleTable = tableEnv
                .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
                .as("name", "score")
                .groupBy($("name"))
                .select($("name"), $("score").sum());

        tableEnv
                .toChangelogStream(simpleTable)
                .executeAndCollect()
                .forEachRemaining(System.out::println);
    }

    private static void changelogStream(StreamExecutionEnvironment streamEnv, StreamTableEnvironment tableEnvironment) {
        DataStreamSource<Row> dataStreamSource = streamEnv.fromElements(Row.ofKind(RowKind.INSERT, "hsm", 12),
                Row.ofKind(RowKind.UPDATE_AFTER, "zs", 11),
                Row.ofKind(RowKind.UPDATE_BEFORE, "ls", 13),
                Row.ofKind(RowKind.DELETE, "ww", 13));
        Table table = tableEnvironment.fromChangelogStream(dataStreamSource,
                Schema.newBuilder().primaryKey("f0").build(),
                ChangelogMode.all()).as("name", "age");
        table.execute().print();
    }

    private static void sql(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("CREATE TABLE GeneratedTable "
                + "("
                + "  name STRING,"
                + "  score INT,"
                + "  event_time TIMESTAMP_LTZ(3),"
                + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                + ")"
                + "WITH ('connector'='datagen')");
        Table table = tableEnvironment.from("GeneratedTable");
        table.execute().print();
    }
}
