package dev.learn.flink.tablesql.function;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: UseFunctionDemo.java
 * @description: UseFunctionDemo.java类说明
 * @author: by echo huang
 * @date: 2021/3/14 5:58 下午
 */
public class UseFunctionDemo {
    public static void main(String[] args) {
        TableEnvironment batchEnv = StreamEnvironment.getBatchEnv();

        Table table = batchEnv.fromValues(ROW(FIELD("id", INT())), row(1), row(2));

//        batchEnv.createTemporaryFunction("Parse",ScalarFunctionDemo.class);
        batchEnv.createTemporaryFunction("Test",TableFunctionDemo.class);


        batchEnv.executeSql("select * from "+table+" LEFT JOIN LATERAL TABLE(Test(id)) ON TRUE").print();
    }
}
