package dev.learn.flink.watermark;

import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @fileName: UseWaterMark.java
 * @description: 使用waterMark
 * @author: by echo huang
 * @date: 2020/8/31 8:50 下午
 */
public class UseWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> datasource = env.fromElements(1, 2, 3, 4);

        //指定时间戳
        datasource.map(data -> data + 1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)))
                .print();

        //todo 自定义
        datasource.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(
                new WatermarkGeneratorSupplier<Integer>() {
                    @Override
                    public WatermarkGenerator<Integer> createWatermarkGenerator(Context context) {
                        return new BoundedOutOfOrdernessWatermarks<>(Duration.ZERO);
                    }
                }
        ));

        env.execute();
    }
}
