package dev.learn.flink;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @fileName: FlinkWordCount.java
 * @description: 快速写个wordcount
 * @author: huangshimin
 * @date: 2022/3/3 10:56 AM
 */
public class FlinkWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> ds = streamEnv.fromCollection(Lists.newArrayList("flink", "hudi"
                , "spark", "iceberg", "hadoop", "yarn", "ds",
                "flink", "spark", "hadoop", "flink"));

        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = ds.map(new MapFunction<String,
                Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
            }
        });

        reduce.print("sink1");
        reduce.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                System.out.println("sink2" + value);
            }
        });

        streamEnv.execute();
    }
}
