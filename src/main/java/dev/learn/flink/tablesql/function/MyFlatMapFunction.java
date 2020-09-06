package dev.learn.flink.tablesql.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @fileName: MyFlatMapFunction.java
 * @description: MyFlatMapFunction.java类说明
 * @author: by echo huang
 * @date: 2020/9/5 1:18 下午
 */
@FunctionHint(output = @DataTypeHint(value = "ROW<word string,name string>"))
public class MyFlatMapFunction extends TableFunction<Row> {
    public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Integer num) {
        collect(Row.of("a", num + "~"));
    }
}
