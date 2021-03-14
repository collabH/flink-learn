package dev.learn.flink.tablesql.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @fileName: TableFunctionDemo.java
 * @description: TableFunctionDemo.java类说明
 * @author: by echo huang
 * @date: 2021/3/14 6:05 下午
 */
@FunctionHint(output = @DataTypeHint("row<name string,desc string>"))
public class TableFunctionDemo extends TableFunction<Row> {
    public void eval(@DataTypeHint("int") Integer num) {
        collect(Row.of("a", num + "~"));
    }
}
