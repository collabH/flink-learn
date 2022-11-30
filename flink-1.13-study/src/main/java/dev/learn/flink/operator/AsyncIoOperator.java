package dev.learn.flink.operator;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @fileName: AsyncIoOperator.java
 * @description: 异步io operator
 * @author: huangshimin
 * @date: 2022/11/30 5:50 PM
 */
public class AsyncIoOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        // 应用异步 I/O 转换操作，不启用重试
        AsyncDataStream
                .unorderedWait(stream, new AddAsyncFunction(), 1000, TimeUnit.MILLISECONDS, 100)
                .print();

// 或 应用异步 I/O 转换操作并启用重试
// 通过工具类创建一个异步重试策略, 或用户实现自定义的策略
//        AsyncRetryStrategy asyncRetryStrategy =
//                new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(3, 100L) // maxAttempts=3, fixedDelay=100ms
//                        .retryIfResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
//                        .retryIfException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
//                        .build();

// 应用异步 I/O 转换操作并启用重试
//        DataStream<Tuple2<String, String>> resultStream =
//                AsyncDataStream.unorderedWaitWithRetry(stream, new AsyncDatabaseRequest(), 1000,
//                        TimeUnit.MILLISECONDS, 100, asyncRetryStrategy);
        env.execute();
    }
}
