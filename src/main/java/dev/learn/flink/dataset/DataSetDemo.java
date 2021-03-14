package dev.learn.flink.dataset;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * @fileName: DataSetDemo.java
 * @description: DataSetDemo.java类说明
 * @author: by echo huang
 * @date: 2021/3/13 10:46 下午
 */
public class DataSetDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment dataSetEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> dataSource = dataSetEnv.fromElements(1, 2, 3, 4, 5);
//        DataSource<Tuple2<String, Integer>> dataSource = dataSetEnv.fromElements(Tuple2.of("a", 1), Tuple2.of("a", 1), Tuple2.of("b", 10), Tuple2.of("a", 2));

//        map(dataSource);
//        project(dataSource);

        dataSetEnv.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(), LongWritable.class, Text.class, "test"));

        DataSetUtils.zipWithUniqueId(dataSource)
                .print();
    }


    /**
     * map operator
     *
     * @param source
     */
    static void map(DataSource<Integer> source) throws Exception {
        source.map(x -> x + 1)
                .print();
    }

    /**
     * flatMap
     *
     * @param source
     * @throws Exception
     */
    static void flatMap(DataSource<Integer> source) throws Exception {
        source.flatMap((value, out) -> out.collect(value * 2))
                .print();
    }


    /**
     * mapPartition
     *
     * @param source
     * @throws Exception
     */
    static void mapPartition(DataSource<Integer> source) throws Exception {
        source.mapPartition((values, out) -> {
            for (Integer value : values) {
                out.collect(value + 2);
            }
        })
                .print();
    }


    /**
     * filter
     *
     * @param source
     * @throws Exception
     */
    static void filter(DataSource<Integer> source) throws Exception {
        source.filter(x -> x % 2 == 0)
                .print();
    }

    /**
     * group reduce
     *
     * @param source
     * @throws Exception
     */
    static void groupReduce(DataSource<Tuple2<String, Integer>> source) throws Exception {
        source
                .groupBy("f0")
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();
    }

    /**
     * aggregate
     *
     * @param source
     * @throws Exception
     */
    static void aggregate(DataSource<Tuple2<String, Integer>> source) throws Exception {
        source
                .aggregate(Aggregations.SUM, 1)
                .print();
    }

    /**
     * distinct
     *
     * @param source
     * @throws Exception
     */
    static void distinct(DataSource<Integer> source) throws Exception {
        source.distinct()
                .print();
    }

    /**
     * project
     *
     * @param source
     * @throws Exception
     */
    static void project(DataSource<Tuple2<String, Integer>> source) throws Exception {
        source
                .project(0)
                .print();
    }


}
