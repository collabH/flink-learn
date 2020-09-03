package dev.learn.flink.transform;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @fileName: DataSetOperator.java
 * @description: dataSet operator
 * @author: by echo huang
 * @date: 2020/9/2 5:51 下午
 */
public class DataSetOperator {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> datasource = env.fromElements("a,b,c,d,e,f", "spark,sql,flink", "spark,sql", "flink");

        // map
        datasource.map(data -> Tuple2.of("a", data))
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .print();


        // flatmap
        datasource.flatMap((String data, Collector<String> out) -> {
            String[] arr = data.split(",");
            for (String word : arr) {
                out.collect(word);
            }
        }).returns(Types.STRING)
                .print();

        // mapPartition
        datasource.mapPartition((Iterable<String> data, Collector<String> out) -> {
            for (String datum : data) {
                out.collect(datum);
            }
        }).returns(Types.STRING)
                .print();

        // filter
        datasource.filter(data -> data.length() > 10)
                .print();


        DataSource<Tuple3<String, String, String>> datasource1 = env.fromElements(Tuple3.of("a", "b", "c"), Tuple3.of("d", "e", "f"), Tuple3.of("h", "j", "k"));

        // project
        datasource1.project(0, 1).print();

        // group
        datasource1.groupBy(1)
                .first(2)
                .print();

        // sort
        datasource1.groupBy(0)
                .sortGroup(1, Order.DESCENDING)
                .first(1)
                .print();

        // agg
//        datasource.max(1)
//                .print();

        // hash partition
        datasource1.partitionByHash(0)
                .print();

        // range partition
        datasource1.partitionByRange(0)
                .print();

    }
}
