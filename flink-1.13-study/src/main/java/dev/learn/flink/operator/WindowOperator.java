package dev.learn.flink.operator;

import dev.learn.flink.FlinkEnvUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @fileName: WindowOperator.java
 * @description: 窗口算子
 * @author: huangshimin
 * @date: 2022/11/25 5:24 PM
 */
public class WindowOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        SingleOutputStreamOperator<Event> ds = env.fromElements(Event.builder()
                        .id(1)
                        .name("hello")
                        .time(System.currentTimeMillis() - 3)
                        .count(1L)
                        .build(),
                Event.builder()
                        .id(1)
                        .name("hello1")
                        .time(System.currentTimeMillis() - 4)
                        .count(2L)
                        .build(),
                Event.builder()
                        .id(2)
                        .name("hello2")
                        .time(System.currentTimeMillis() - 3)
                        .count(1L)
                        .build()
                , Event.builder()
                        .id(2)
                        .name("hello3")
                        .time(System.currentTimeMillis() - 4)
                        .count(1L)
                        .build()
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withIdleness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getTime())
        );
        ds.keyBy(new KeySelector<Event, Integer>() {
                    @Override
                    public Integer getKey(Event event) throws Exception {
                        return event.getId();
                    }
                }).window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
                // 增量窗口函数 reduce
//                .reduce(new ReduceFunction<Event>() {
//                    @Override
//                    public Event reduce(Event event, Event t1) throws Exception {
//                        return Event.builder()
//                                .id(event.getId())
//                                .name(t1.getName())
//                                .count(event.getCount() + t1.getCount())
//                                .time(t1.getTime()).build();
//                    }
//                }, new ProcessWindowFunction<Event, Event, Integer, TimeWindow>() {
//                    @Override
//                    public void process(Integer id,
//                                        ProcessWindowFunction<Event, Event, Integer, TimeWindow>.Context context,
//                                        Iterable<Event> iter, Collector<Event> collector) throws Exception {
//                        iter.forEach(collector::collect);
//                    }
//                })
                // agg
//                .aggregate(new AggregateFunction<Event, Long, Event>() {
//                    @Override
//                    public Long createAccumulator() {
//                        return 0L;
//                    }
//
//                    @Override
//                    public Long add(Event event, Long aLong) {
//                        return event.getCount() + aLong;
//                    }
//
//                    @Override
//                    public Event getResult(Long aLong) {
//                        return null;
//                    }
//
//                    @Override
//                    public Long merge(Long aLong, Long acc1) {
//                        return null;
//                    }
//                }, new ProcessWindowFunction<Event, Event, Integer, TimeWindow>() {
//                    @Override
//                    public void process(Integer integer,
//                                        ProcessWindowFunction<Event, Event, Integer, TimeWindow>.Context context,
//                                        Iterable<Event> iterable, Collector<Event> collector) throws Exception {
//
//                    }
//                })
                // 全量窗口
                .process(new ProcessWindowFunction<Event, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer,
                                        ProcessWindowFunction<Event, String, Integer, TimeWindow>.Context context,
                                        Iterable<Event> iterable, Collector<String> collector) throws Exception {
                        System.out.println(integer);
                        for (Event event : iterable) {
                            collector.collect(event.toString());
                        }
                    }
                })
                .print();
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    static class Event {
        private Integer id;
        private String name;
        private Long time;
        private Long count;
    }
}
