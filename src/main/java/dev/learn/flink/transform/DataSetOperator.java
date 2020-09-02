package dev.learn.flink.transform;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @fileName: DataSetOperator.java
 * @description: dataSet operator
 * @author: by echo huang
 * @date: 2020/9/2 5:51 下午
 */
public class DataSetOperator {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> datasource = env.fromElements("a,b,c,d,e,f", "spark,sql,flink", "spark,sql", "flink");

    }
}
