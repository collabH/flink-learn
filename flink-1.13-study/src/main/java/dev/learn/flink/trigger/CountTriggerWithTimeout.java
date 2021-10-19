package dev.learn.flink.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 带超时的计数窗口触发器
 */
public class CountTriggerWithTimeout<T> extends Trigger<T, TimeWindow> {
    private static Logger LOG = LoggerFactory.getLogger(CountAndTimeTrigger.class);
    /**
     * 用于储存窗口当前数据量的状态对象
     */
    private final ReducingStateDescriptor<Long> countStateDescriptor =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);
    /**
     * 窗口最大数据量
     */
    private final int maxCount;
    /**
     * event time / process time
     */
    private final boolean isEventTime;


    private CountTriggerWithTimeout(int maxCount, boolean isEventTime) {


        this.maxCount = maxCount;
        this.isEventTime = isEventTime;
    }

    public static CountTriggerWithTimeout of(int maxCount, boolean isEventTime) {
        return new CountTriggerWithTimeout(maxCount, isEventTime);
    }

    private TriggerResult fireAndPurge(TimeWindow window, TriggerContext ctx) throws Exception {
        clear(window, ctx);
        return TriggerResult.FIRE_AND_PURGE;
    }


    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
        countState.add(1L);


        if (countState.get() >= maxCount) {
            LOG.info("fire with count: " + countState.get());
            return fireAndPurge(window, ctx);
        }
        if (timestamp >= window.getEnd()) {
            LOG.info("fire with time: " + timestamp);
            return fireAndPurge(window, ctx);
        } else {
            return TriggerResult.CONTINUE;
        }
    }


    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (isEventTime) {
            return TriggerResult.CONTINUE;
        }
        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            LOG.info("fire with process time: " + time);
            return fireAndPurge(window, ctx);
        }
    }


    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (!isEventTime) {
            return TriggerResult.CONTINUE;
        }
        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            LOG.info("fire with event time: " + time);
            return fireAndPurge(window, ctx);
        }
    }


    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(countStateDescriptor).clear();
    }

    public static class Sum implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}
