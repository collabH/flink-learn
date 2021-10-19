package dev.learn.flink.trigger;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Map;

/**
 * @fileName: TimeSourceFunction.java
 * @description: TimeSourceFunction.java类说明
 * @author: huangshimin
 * @date: 2021/10/19 2:40 下午
 */
public class TimeSourceFunction implements SourceFunction<Tuple2<String, String>> {
    private static final Map<Integer,String> KEYS=Maps.newHashMap();
    static {
        KEYS.put(1,"a");
        KEYS.put(2,"b");
        KEYS.put(3,"c");
        KEYS.put(4,"d");
    }
    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        while (true) {
            sourceContext.collect(Tuple2.of(KEYS.get(RandomUtils.nextInt(1,4)),"haha" + RandomUtils.nextInt(10, 1000)));
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {

    }
}
