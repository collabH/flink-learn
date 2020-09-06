package dev.learn.flink.tablesql.table;

import dev.learn.flink.tablesql.function.MyAggFunction;
import dev.learn.flink.tablesql.function.MyFlatMapFunction;
import dev.learn.flink.tablesql.function.MyMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: UseRowOperator.java
 * @description: UseRowOperator.java类说明
 * @author: by echo huang
 * @date: 2020/9/4 11:56 下午
 */
public class UseRowOperator {
    public static void main(String[] args) {
        TableEnvironment batchEnv = StreamEnvironment.getBatchEnv();

        Table table = batchEnv.fromValues(ROW(FIELD("id", INT())), row(1), row(2));

        batchEnv.createFunction("func", MyMapFunction.class);

        // map
        table.map(call("func", $("id")))
                .execute().print();

        // fixme 源码中的flatmap只支持传入TableFunctionDefinition的子类，但是TableFunctionDefinition不运行继承
        MyFlatMapFunction func = new MyFlatMapFunction();

        //flatmap
//        table.flatMap(call(func,$("id")))
//                .execute().print();

        // aggregate
        MyAggFunction myAggFunction = new MyAggFunction();
        table.groupBy($("id"))
                .aggregate(call(myAggFunction, $("id")).as("agg"))
                .select($("agg"))
                .execute().print();
    }
}