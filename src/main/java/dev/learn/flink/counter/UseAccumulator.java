package dev.learn.flink.counter;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: UseAccumulator.java
 * @description: 使用Accumulator
 * @author: by echo huang
 * @date: 2020/9/1 5:05 下午
 */
public class UseAccumulator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> datasource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        datasource.map(new AccumulatorFunction())
                .print();

        JobExecutionResult result = env.execute();

        // 获取累加器
        Object accumulatorResult = result.getAccumulatorResult("sum-lines");
        System.out.println(accumulatorResult);
    }
}

class AccumulatorFunction extends RichMapFunction<Integer, Integer> {
    private IntCounter counter;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化计数器
        counter = new IntCounter();
        // 计数器放入flink上下文中
        getRuntimeContext().addAccumulator("sum-lines", counter);
    }

    @Override
    public Integer map(Integer value) throws Exception {
        // 统计flink处理数据量
        this.counter.add(1);

        return value * 2;
    }
}

