package dev.learn.flink.tablesql.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @fileName: ScalarFunctionDemo.java
 * @description: ScalarFunctionDemo.java类说明
 * @author: by echo huang
 * @date: 2021/3/14 5:55 下午
 */
@FunctionHint(output = @DataTypeHint("row<word string,name string>"))
public class ScalarFunctionDemo extends ScalarFunction {

    public Row eval(@DataTypeHint(value = "int") Integer num) {
        return Row.of("word", num + "~");
    }
}
