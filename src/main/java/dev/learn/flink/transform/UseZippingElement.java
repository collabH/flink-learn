package dev.learn.flink.transform;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.DataSetUtils;

/**
 * @fileName: Use.java
 * @description: Use.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 10:01 上午
 */
public class UseZippingElement {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> datasource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        // zip with index
        DataSetUtils.zipWithIndex(datasource)
                .print();

        DataSource<String> datasource1 = env.fromElements("A", "C", "B", "D", "H", "E");

        // zip uniqueId
        DataSetUtils.zipWithUniqueId(datasource1)
                .print();

    }

}
