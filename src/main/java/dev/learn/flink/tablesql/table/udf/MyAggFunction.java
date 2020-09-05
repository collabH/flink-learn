package dev.learn.flink.tablesql.table.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @fileName: MyAggFunction.java
 * @description: MyAggFunction.java类说明
 * @author: by echo huang
 * @date: 2020/9/5 4:33 下午
 */
public class MyAggFunction extends AggregateFunction<Row, AggDomain> {
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.INT);
    }

    public void accumulate(AggDomain a, Integer value) {
        if (value < a.getMax()) {
            a.setMax(value);
        }
    }

    @Override
    public Row getValue(AggDomain a) {
        return Row.of(a.getMax());
    }

    @Override
    public AggDomain createAccumulator() {
        return new AggDomain();
    }
}

class AggDomain {
    private Integer max;

    public Integer getMax() {
        return max;
    }

    public void setMax(Integer max) {
        this.max = max;
    }
}