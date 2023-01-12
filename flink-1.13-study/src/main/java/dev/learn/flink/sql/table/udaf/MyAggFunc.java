package dev.learn.flink.sql.table.udaf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.annotation.HintFlag.TRUE;

public class MyAggFunc extends AggregateFunction<Row, MinMaxAcc> {

    @Override
    public Row getValue(MinMaxAcc accumulator) {
        return Row.of(accumulator.max, accumulator.min);
    }


    @Override
    public MinMaxAcc createAccumulator() {
        return new MinMaxAcc();
    }

    @FunctionHint(
            accumulator = @DataTypeHint(bridgedTo = MinMaxAcc.class, allowRawGlobally = TRUE),
            input = @DataTypeHint("int"),
            output = @DataTypeHint("row<max int,min int>")
    )
    public void accumulate(MinMaxAcc minMaxAcc, Integer value) {
        if (minMaxAcc.max < value) {
            minMaxAcc.max = value;
        }
        if (minMaxAcc.min > value) {
            minMaxAcc.min = value;
        }
    }

}