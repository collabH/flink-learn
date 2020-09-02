package dev.learn.flink.process;

import dev.learn.flink.domain.MessageEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author babywang
 */
public class AlarmProcessFunction extends KeyedProcessFunction<String, MessageEvent, String> {
    private transient ValueState<Integer> lastTemperature;
    private transient ValueState<Long> lastTimerStamp;
    private final Long interval;

    public AlarmProcessFunction(Long interval) {
        this.interval = interval;
    }

    /**
     * 连续interval秒
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(MessageEvent value, Context ctx, Collector<String> out) throws Exception {
        ctx.output(new OutputTag<Tuple2<String, String>>("side-output", TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        })){}, Tuple2.of(value.getId(), value.getTemperature() + "C"));
        // 当前key
        String currentKey = ctx.getCurrentKey();

        // 时间服务器
        TimerService timerService = ctx.timerService();

        long processingTime = timerService.currentProcessingTime();

        // 上次温度
        Integer lastTemp = lastTemperature.value();

        // init last temp state
        if (lastTemp == null) {
            // 更新温度状态
            lastTemperature.update(Integer.parseInt(value.getTemperature()));
            out.collect(currentKey + ":" + value.getTemperature() + ":" + value.getTimestamp());
            return;
        }
        Long lastTimerTimestamp = lastTimerStamp.value();
        // 比较当前温度,如果上次温度大于当前问题,移除定时器
        if (lastTemp >= Integer.parseInt(value.getTemperature())) {
            lastTemperature.update(Integer.parseInt(value.getTemperature()));
            if (lastTimerTimestamp == null) {
                out.collect(currentKey + ":" + value.getTemperature() + ":" + value.getTimestamp());
                return;
            }
            timerService.deleteProcessingTimeTimer(lastTimerTimestamp);
            out.collect(currentKey + ":" + value.getTemperature() + ":" + value.getTimestamp());
            return;
        }
        // 只有当上次不存在定时器时才进行注册
        if (lastTimerTimestamp == null) {
            timerService.registerProcessingTimeTimer(processingTime + this.interval);
            lastTimerStamp.update(processingTime + this.interval);
        }
        out.collect(currentKey + ":" + value.getTemperature() + ":" + value.getTimestamp());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemperature-record", Integer.class));
        lastTimerStamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTimerStamp", Long.class));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        String currentKey = ctx.getCurrentKey();
        out.collect("id:" + currentKey + " 近" + this.interval + "ms内连续出现升温现象");
        // 情况该定时器
        lastTimerStamp.clear();
    }

    @Override
    public void close() throws Exception {
        lastTimerStamp.clear();
        lastTemperature.clear();
    }
}