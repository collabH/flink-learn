package dev.learn.flink.transform;

import dev.learn.flink.function.SumReduceFunction;
import dev.learn.flink.function.WordCountMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @fileName: KeyOperrator.java
 * @description: KeyOperrator.java类说明
 * @author: by echo huang
 * @date: 2020-08-29 23:28
 */
public class KeyOperrator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> datasource = env.fromElements("hello,hsm,zhangsan,hsm,hello,sz,ls,sz,li,hsm");

        datasource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String s1 : s.split(",")) {
                    collector.collect(s1);
                }
            }
        }).map(new WordCountMapFunction())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }).reduce(new SumReduceFunction())
                .print();

        env.execute();
    }
}
