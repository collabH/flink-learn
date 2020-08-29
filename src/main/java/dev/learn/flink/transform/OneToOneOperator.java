package dev.learn.flink.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @fileName: OneToOneOperator.java
 * @description: one to one operator
 * @author: by echo huang
 * @date: 2020-08-29 22:50
 */
public class OneToOneOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3, 4, 5);


        integerDataStreamSource.map(integer -> integer + 1)
                .filter(i -> i > 1)
                .flatMap(new FlatMapFunction<Integer, String>() {
                    @Override
                    public void flatMap(Integer integer, Collector<String> collector) throws Exception {
                        collector.collect(integer + "hhhh");
                    }
                })
                .print();


        executionEnvironment.execute();
    }
}
