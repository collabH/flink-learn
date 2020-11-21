package dev.learn.flink.window.evictor;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

/**
 * @fileName: EvictorDemo.java
 * @description: EvictorDemo.java类说明
 * @author: by echo huang
 * @date: 2020/11/21 4:43 下午
 */
public class EvictorDemo {
    /**
     * timeEvictor
     */
    public void timeEvictor() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 23, 4, 5, 6)
                .keyBy((KeySelector<Integer, Integer>) value -> value)
                .window(GlobalWindows.create())
                // 只看最近5分钟的记录
                .evictor(TimeEvictor.of(Time.minutes(5)))
                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                .max(1);
    }
}
