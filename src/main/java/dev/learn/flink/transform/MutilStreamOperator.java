package dev.learn.flink.transform;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @fileName: MutilStreamOperator.java
 * @description: 多流算子
 * @author: by echo huang
 * @date: 2020-08-30 00:39
 */
public class MutilStreamOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Tuple2<String, Integer>> datasource =
                env.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2), Tuple2.of("b", 1), Tuple2.of("a", 2));

        DataStreamSource<Tuple2<String, Integer>> datasource1 =
                env.fromElements(Tuple2.of("a", 3), Tuple2.of("b", 4));

        // 分流

//        datasource.split(new OutputSelector<Tuple2<String, Integer>>() {
//            @Override
//            public Iterable<String> select(Tuple2<String, Integer> data) {
//                if ("a".equals(data.f0)) {
//                    return Lists.newArrayList("high");
//                } else {
//                    return Lists.newArrayList("low");
//                }
//            }
//        }).select("high")
//                .print("high");


        // 合流
        datasource.connect(datasource1)
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }, new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }).map(new CoMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String,
                Integer>>() {

            private Tuple2<String, Integer> tuple2;

            @Override
            public Tuple2<String, Integer> map1(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                tuple2 = stringIntegerTuple2;
                return null;
            }

            @Override
            public Tuple2<String, Integer> map2(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {

                return Tuple2.of(stringIntegerTuple2.f0, tuple2.f1 + stringIntegerTuple2.f1);

            }
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        })).print();

        env.execute();

    }
}
