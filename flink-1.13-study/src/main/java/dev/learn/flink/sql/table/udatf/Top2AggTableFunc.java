package dev.learn.flink.sql.table.udatf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;

/**
 * @fileName: Top2AggTableFunc.java
 * @description: Top2AggTableFunc.java类说明
 * @author: huangshimin
 * @date: 2023/1/12 8:44 PM
 */
public class Top2AggTableFunc extends TableAggregateFunction<Row, Tuple2<Integer, Integer>> {
    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return new Tuple2<>();
    }

    @DataTypeHint
    public void accumulator(Tuple2<Integer, Integer> acc, Integer value) {

    }

}
