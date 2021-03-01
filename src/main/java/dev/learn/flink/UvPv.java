package dev.learn.flink;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @fileName: UvPv.java
 * @description: UvPv.java类说明
 * @author: by echo huang
 * @date: 2020/10/8 6:11 下午
 */
public class UvPv {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<DomainTest> datasource = env.fromCollection(Lists.newArrayList(new DomainTest("1"), new DomainTest("2"), new DomainTest("3"),
                new DomainTest("1"), new DomainTest("2"), new DomainTest("1"), new DomainTest("3")));

        // 一种方法使用全量窗口 uv和pv一块计算
        datasource.timeWindowAll(Time.minutes(5))
                .process(new ProcessAllWindowFunction<DomainTest, Result, TimeWindow>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }

                    @Override
                    public void process(Context context, Iterable<DomainTest> elements, Collector<Result> out) throws Exception {
                        // 计算uv和pv
                        // 这里可用bloomFilter,基于内存的可能会在大流量下出现oom
                        HashSet<String> uvs = new HashSet<>();
                        long pv = 0L;
                        for (DomainTest element : elements) {
                            pv++;
                            uvs.add(element.getUserId());
                        }
                        out.collect(new Result((long) uvs.size(), pv));
                    }
                });

        // 分别计算,先使用aggreator算子分别预计算

        // 使用timer
        datasource.process(new ProcessFunction<DomainTest, Result>() {

            private transient ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Long> timestamp = new ValueStateDescriptor<>("timestamp", Long.class);
                timer = getRuntimeContext().getState(timestamp);
            }

            @Override
            public void processElement(DomainTest value, Context ctx, Collector<Result> out) throws Exception {
                TimerService timerService = ctx.timerService();
                long currentProcessingTime = timerService.currentProcessingTime();
                long time = currentProcessingTime + 5 * 1000 * 60;
                timerService.registerProcessingTimeTimer(time);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Result> out) throws Exception {

            }
        });
    }
}


class Result {
    private Long uv;
    private Long pv;

    public Result(Long uv, Long pv) {
        this.uv = uv;
        this.pv = pv;
    }

    public Long getUv() {
        return uv;
    }

    public void setUv(Long uv) {
        this.uv = uv;
    }

    public Long getPv() {
        return pv;
    }

    public void setPv(Long pv) {
        this.pv = pv;
    }
}

class DomainTest {
    private String userId;

    public DomainTest(String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
