package dev.learn.flink.cep;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @fileName: SpiteLoginMonitor.java
 * @description: 恶意登陆监控
 * @author: by echo huang
 * @date: 2020/9/11 8:57 下午
 */
public class SpiteLoginMonitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KeyedStream<LoginEvent, String> loginEventStream = env.socketTextStream("hadoop", 9999)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    private long maxTimestamp = 0L;

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTimestamp - 5000);
                    }

                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] arr = element.split(",");
                        maxTimestamp = Math.max(maxTimestamp, Long.parseLong(arr[2]));
                        return maxTimestamp;
                    }
                }).map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new LoginEvent(arr[0], arr[1], Long.parseLong(arr[2]));
                    }
                }).returns(TypeInformation.of(LoginEvent.class)).keyBy(new KeySelector<LoginEvent, String>() {
                    @Override
                    public String getKey(LoginEvent value) throws Exception {
                        return value.getUserId();
                    }
                });

        // 定义CEP模式和流
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream, Pattern.<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginStatus());
                    }
                }).next("secondFail").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginStatus());
                    }
                }).within(Time.seconds(2)));

        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent firstFail = map.getOrDefault("firstFail", Lists.newArrayList()).get(0);
                LoginEvent secondFail = map.getOrDefault("secondFail", Lists.newArrayList()).get(0);
                return "userId:" + firstFail.getUserId() + " firstFail:" + firstFail.getEventTimestamp() + "---" + secondFail.getEventTimestamp();
            }
        }).print();

        env.execute("login fail with cep");
    }
}

@Data
@AllArgsConstructor
class LoginEvent {
    private String userId;
    private String loginStatus;
    private Long eventTimestamp;
}
