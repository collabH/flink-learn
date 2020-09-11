package dev.learn.flink.cep;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @fileName: OrderPayDetect.java
 * @description: OrderPayDetect.java类说明
 * @author: by echo huang
 * @date: 2020/9/11 11:58 下午
 */
public class OrderPayDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<OrderLog, String> orderStream = env.fromElements("1,create,,1599406700000","1,pay,xxx,1599406740000",
                "2,create,,1599406700000","2,pay,xxx,1599406710000")
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    private long maxTimestamp = 0L;

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTimestamp - 5000);
                    }

                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] arr = element.split(",");
                        maxTimestamp = Math.max(maxTimestamp, Long.parseLong(arr[3]));
                        return maxTimestamp;
                    }
                }).map(new MapFunction<String, OrderLog>() {
                    @Override
                    public OrderLog map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new OrderLog(arr[0], arr[1], arr[2], Long.parseLong(arr[3]));
                    }
                })
                .returns(TypeInformation.of(OrderLog.class))
                .keyBy(new KeySelector<OrderLog, String>() {
                    @Override
                    public String getKey(OrderLog value) throws Exception {
                        return value.getOrderId();
                    }
                });

        // 定于模式
        Pattern<OrderLog, OrderLog> parttern = Pattern.<OrderLog>begin("create")
                .where(new SimpleCondition<OrderLog>() {
                    @Override
                    public boolean filter(OrderLog value) throws Exception {
                        return "create".equals(value.getOrderStatus());
                    }
                }).followedBy("payOrder")
                .where(new SimpleCondition<OrderLog>() {
                    @Override
                    public boolean filter(OrderLog value) throws Exception {
                        return "pay".equals(value.getOrderStatus());
                    }
                    // fixme 假如30秒为超时订单
                }).within(Time.seconds(30));

        OutputTag<OrderResult> outputTag = new OutputTag<>("timeout-order", TypeInformation.of(OrderResult.class));
        PatternStream<OrderLog> orderLogPatternStream =
                CEP.pattern(orderStream, parttern);

        // 筛选数据
        SingleOutputStreamOperator<String> outStream = orderLogPatternStream.select(outputTag, new PatternTimeoutFunction<OrderLog, OrderResult>() {
            @Override
            public OrderResult timeout(Map<String, List<OrderLog>> map, long timestamp) throws Exception {
                OrderLog timeOutOrder = map.get("create").get(0);
                OrderResult orderResult = new OrderResult();
                orderResult.setOrderId(timeOutOrder.getOrderId());
                orderResult.setOrderStatus(timeOutOrder.getOrderStatus());
                orderResult.setTimeoutTimeStamp(timestamp);
                return orderResult;
            }
        }, new PatternSelectFunction<OrderLog, String>() {
            @Override
            public String select(Map<String, List<OrderLog>> map) throws Exception {
                //fixme 正常订单不做处理
                return JSON.toJSONString(map);
            }
        });

        outStream.print();

        outStream.getSideOutput(outputTag)
                .print();

        env.execute();

    }
}

@Data
class OrderResult {
    private String orderId;
    private String orderStatus;
    private Long timeoutTimeStamp;
}

@Data
@AllArgsConstructor
class OrderLog {
    private String orderId;
    private String orderStatus;
    private String txId;
    private Long eventTimestamp;
}