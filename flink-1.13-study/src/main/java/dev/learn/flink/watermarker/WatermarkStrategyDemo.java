package dev.learn.flink.watermarker;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;

/**
 * @fileName: WatermarkStrategyDemo.java
 * @description: WatermarkStrategyDemo.java类说明
 * @author: huangshimin
 * @date: 2022/11/14 8:11 PM
 */
public class WatermarkStrategyDemo {
    public static void main(String[] args) {
        // 定义watermark策略
        WatermarkStrategy<Tuple2<Long, String>> watermarkStrategy = WatermarkStrategy
                // 允许延迟5秒的数据
                .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 如果有subtask 10s没有watermarker推进 多流进行watermarker选择则过滤改subtask
                .withIdleness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.f0);
        // 分区对齐，防止因空闲数据过滤对应watermark导致其数据处理过慢，最终导致数据cache过大问题
        //.withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(1));


    }
}
