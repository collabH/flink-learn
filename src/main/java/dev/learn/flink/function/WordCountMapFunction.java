package dev.learn.flink.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * @fileName: WordCountMapFunction.java
 * @description: WordCountMapFunction.java类说明
 * @author: by echo huang
 * @date: 2020-08-27 22:48
 */
public class WordCountMapFunction implements MapFunction<String, Tuple2<String, Integer>>, ResultTypeQueryable<Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String s) throws Exception {
        return Tuple2.of(s, 1);
    }

    @Override
    public TypeInformation<Tuple2<String, Integer>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        });
    }
}
