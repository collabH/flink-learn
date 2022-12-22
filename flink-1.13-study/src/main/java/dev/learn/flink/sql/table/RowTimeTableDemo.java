package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

/**
 * @fileName: RowTimeTableDemo.java
 * @description: rowtime通过table api/flink sql定义方式
 * @author: huangshimin
 * @date: 2022/12/22 6:59 PM
 */
public class RowTimeTableDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // support rowtime
        DataStream<User> dataStream =
                streamEnv.fromElements(
                        new User("Alice", 4, Instant.ofEpochMilli(1000)),
                        new User("Bob", 6, Instant.ofEpochMilli(1001)),
                        new User("Alice", 10, Instant.ofEpochMilli(1002)));

        // 使用domain方式定义timestamp字段
        tableEnv.fromDataStream(dataStream)
                .printSchema();
        /***
         * (
         *   `name` STRING,
         *   `score` INT,
         *   `event_time` TIMESTAMP_LTZ(9)
         * )
         */

        // 指定processing time
        tableEnv.fromDataStream(dataStream, Schema.newBuilder()
                        .columnByExpression("proctime", "PROCTIME()").build())
                .printSchema();
        /**
         * (
         *   `name` STRING,
         *   `score` INT,
         *   `event_time` TIMESTAMP_LTZ(9),
         *   `proctime` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
         * )
         */
        // 指定event time
        tableEnv.fromDataStream(dataStream, Schema.newBuilder()
                        .columnByExpression("rowtime", "cast(event_time as TIMESTAMP_LTZ(3))")
                        .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                        .build())
                .printSchema();
        /**
         * (
         *   `name` STRING,
         *   `score` INT,
         *   `event_time` TIMESTAMP_LTZ(9),
         *   `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS cast(event_time as TIMESTAMP_LTZ(3)),
         *   WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
         * )
         */
        // 使用source watermark
        tableEnv.fromDataStream(dataStream, Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("score", DataTypes.INT())
                        .column("event_time", "TIMESTAMP_LTZ(3)")
                        .watermark("event_time", Expressions.sourceWatermark())
                        .build())
                .printSchema();
        /**
         * (
         *   `name` STRING,
         *   `score` INT,
         *   `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
         *   WATERMARK FOR `event_time`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
         * )
         */

        System.out.println("-------------type------------------");
        // -------- type
        DataStreamSource<User1> user1 = streamEnv.fromElements(new User1("hsm", 100),
                new User1("ls", 90),
                new User1("zs", 80));
        tableEnv.fromDataStream(user1, Schema.newBuilder()
                        .column("f0", DataTypes.of(User1.class))
                        .build()).as("user")
                .printSchema();
        /**
         * (
         *   `user` *dev.learn.flink.sql.table.RowTimeTableDemo$User1<`name` STRING, `score` INT>*
         * )
         */
        tableEnv.fromDataStream(user1, Schema.newBuilder()
                        .column("f0", DataTypes.STRUCTURED(User1.class,
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("score", DataTypes.INT())))
                        .build()).as("user")
                .printSchema();
        /**
         * (
         *   `user` *dev.learn.flink.sql.table.RowTimeTableDemo$User1<`name` STRING, `score` INT>*
         * )
         */

    }

    public static class User {
        public String name;

        public Integer score;

        public Instant event_time;

        // 指定默认构造方法，datastream可以自动推断对应属性字段
        public User() {
        }

        // fully assigning constructor for Table API
        public User(String name, Integer score, Instant event_time) {
            this.name = name;
            this.score = score;
            this.event_time = event_time;
        }
    }

    public static class User1 {
        public String name;

        public Integer score;

        // fully assigning constructor for Table API
        public User1(String name, Integer score) {
            this.name = name;
            this.score = score;
        }
    }
}
