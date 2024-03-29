package dev.learn.flink.feature;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @fileName: OperatorFeature.java
 * @description: 算子特性
 * @author: huangshimin
 * @date: 2021/8/22 4:21 下午
 */
public class OperatorFeature {

    /**
     * 窗口
     */
    public static void window() {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<Integer> ds = streamEnv.fromElements(1, 2, 3, 4, 5);


        ds.keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .aggregate(new AggregateFunction<Integer, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return null;
                    }

                    @Override
                    public Integer add(Integer value, Integer accumulator) {
                        return null;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return null;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                }, new ProcessWindowFunction<Integer, Integer, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> iterable,
                                        Collector<Integer> collector) throws Exception {
                    }
                });
    }

    /**
     * join
     */
    public static void join() throws Exception {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();

        // window join
//        stream.join(otherStream)
//                .where(<KeySelector>)
//                .equalTo(<KeySelector>)
//                .window(<WindowAssigner>)
//                .apply(<JoinFunction>)
        // tumbling window join
        SingleOutputStreamOperator<Integer> ds1 =
                streamEnv.fromElements(0, 1);

        SingleOutputStreamOperator<Integer> ds2 =
                streamEnv.fromElements(0, 1);


        // 俩个都不生效，强制指定watermark
        ds1.join(ds2)
                .where(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).equalTo(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.milliseconds(100)))
                .apply(new JoinFunction<Integer, Integer, String>() {
                    @Override
                    public String join(Integer first, Integer second) throws Exception {
                        return first + "," + second;
                    }
                }).print();

        //sliding join
        ds1.join(ds2)
                .where(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).equalTo(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).window(SlidingEventTimeWindows.of(Time.milliseconds(2) /* size */, Time.milliseconds(1) /* slide */))
                .apply(new JoinFunction<Integer, Integer, String>() {
                    @Override
                    public String join(Integer first, Integer second) {
                        return first + "," + second;
                    }
                });

        // internal join
        ds1.keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).intervalJoin(ds2.keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })).between(Time.seconds(10), Time.seconds(20))
                .process(new ProcessJoinFunction<Integer, Integer, String>() {
                    @Override
                    public void processElement(Integer integer, Integer integer2, Context context,
                                               Collector<String> collector) throws Exception {
                        collector.collect(integer + "," + integer2);
                    }
                }).print();
        streamEnv.execute();
    }


    public static void asyncIo() {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        SingleOutputStreamOperator<Integer> asyncIoStream =
                AsyncDataStream.unorderedWait(streamEnv.fromElements(1, 23, 4), new AsyncFunction<Integer, Integer>() {
                    @Override
                    public void asyncInvoke(Integer value, ResultFuture<Integer> resultFuture) throws Exception {
                        Future<Integer> result = FutureUtils.get(value);
                        CompletableFuture.supplyAsync(new Supplier<Integer>() {
                            @Override
                            public Integer get() {
                                try {
                                    return result.get();
                                } catch (InterruptedException | ExecutionException e) {
                                    return null;
                                }
                            }
                        }).thenAcceptAsync(new Consumer<Integer>() {
                            @Override
                            public void accept(Integer value) {
                                resultFuture.complete(Collections.singleton(value));
                            }
                        });
                    }
                }, 1, TimeUnit.HOURS, 100);
    }

    public static void main(String[] args) throws Exception {
//        join();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.out.println("t:" + t.getName() + " e:" + e.getMessage());
            }
        });

        throw new RuntimeException("this is exeception");
    }

    static class FutureUtils {
        public static Future<Integer> get(Integer key) {
            return new FutureTask<>(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return key + 1;
                }
            });
        }
    }
}
