package dev.learn.flink.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * @fileName: SumReduceFunction.java
 * @description: SumReduceFunction.java类说明
 * @author: by echo huang
 * @date: 2020-08-27 22:45
 */
public class SumReduceFunction implements ReduceFunction<Tuple2<String, Integer>>, ResultTypeQueryable<Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> d1, Tuple2<String, Integer> d2) throws Exception {
        return Tuple2.of(d1.f0, d1.f1 + d2.f1);
    }


    @Override
    public TypeInformation<Tuple2<String, Integer>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        });
    }
}
