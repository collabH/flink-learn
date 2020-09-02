package dev.learn.flink.lambda;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @fileName: UseLambda.java
 * @description: use lambda
 * @author: by echo huang
 * @date: 2020/9/2 4:40 下午
 */
public class UseLambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> datasource = env.fromElements(1, 2, 3, 4, 5, 6);

        datasource.map(data -> Tuple2.of("a", data))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();

        datasource.flatMap((Integer data, Collector<Integer> out) -> {
            out.collect(data * 2);
        }).returns(Types.INT)
                .print();

        env.execute();
    }
}
