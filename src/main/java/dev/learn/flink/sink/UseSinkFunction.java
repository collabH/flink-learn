package dev.learn.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @fileName: UseSinkFunction.java
 * @description: sink方式
 * @author: by echo huang
 * @date: 2020-08-30 13:38
 */
public class UseSinkFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> datasource = env.fromElements("hh", "Sd", "Dsd", "4, 5", "6")
                .setParallelism(1);

        // print
        datasource.print();

        // text
//        datasource.writeAsText("/Users/babywang/Documents/reserch/dev/forchange/workspace/flink-learn/src/main/resources/data", FileSystem.WriteMode.OVERWRITE);
//        datasource.writeToSocket("hadoop", 9999, new SimpleStringSchema());

        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path("hdfs://hadoop:8020/flink1.11.1/data"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                // 指定分桶分配器
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd"))
                .build();
        datasource.addSink(sink);
        env.execute();
    }
}
