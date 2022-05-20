package dev.learn.flink.sql.ds2table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @fileName: DatastreamToTableFeature.java
 * @description: datastream转换table
 * @author: huangshimin
 * @date: 2021/8/28 9:04 下午
 */
public class DatastreamToTableFeature {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(env,
                        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        DataStream<Tuple3<String, Integer, Long>> dataStream = env.fromElements(
                Tuple3.of("Alice", 12,System.currentTimeMillis()+1000),
                Tuple3.of("Bob", 10, System.currentTimeMillis()+1200),
                Tuple3.of("Alice", 100, System.currentTimeMillis()+2300));
        // 设置时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        // new api
//        tableEnv.fromDataStream(dataStream,Schema.newBuilder().build());
        Table table = tableEnv.fromDataStream(dataStream, $("f0").as("name"), $("f1").as("age"),
                $("f2").as("event_time"));

        table.execute().print();
        env.execute();
    }

    private static Table dsToTable(StreamTableEnvironment tableEnv, DataStream<Row> dataStream) {
        Table table =
                tableEnv.fromDataStream(dataStream,
                        Schema.newBuilder()
                                .columnByExpression("proc_time", "PROCTIME()")
                                // 提取row time
                                .columnByExpression("rowtime", "CAST(f2 as timestamp_ltz(3))")
                                // watermark
                                .watermark("rowtime", "rowtime-INTERVAL '10' SECOND").build())
                        .as("name", "age", "event_time");
        return table;
    }

    private static void tableToDs(StreamTableEnvironment tableEnv, Table table) {
        tableEnv.createTemporaryView("test", table);

        Table tableView = tableEnv.sqlQuery("select * from test");
        // table转ds
        tableEnv.toChangelogStream(tableView).print();
        tableEnv.toRetractStream(tableView, TypeInformation.of(Row.class)).print();
        tableEnv.toAppendStream(tableView, TypeInformation.of(Row.class)).print();
    }

}
