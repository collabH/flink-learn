package dev.learn.flink.asyncio;

import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @fileName: UseAsyncIO.java
 * @description: 使用 async io
 * @author: by echo huang
 * @date: 2020/9/2 10:50 上午
 */
public class UseAsyncIO {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> datasource = env.fromElements("async", "flink", "spark", "flink sql", "spark sql");

        /**
         * capacity: 线程池中任务队列任务总个数
         * unorderedWait: 无序模式
         * orderedWait: 有序模式, 这种模式保持了流的顺序。发出结果记录的顺序与触发异步请求的顺序（记录输入算子的顺序）相同。为了实现这一点，算子将缓冲一个结果记录直到这条记录前面的所有记录都发出（或超时）。由于记录或者结果要在 checkpoint 的状态中保存更长的时间，所以与无序模式相比，有序模式通常会带来一些额外的延迟和 checkpoint 开销。
         */
        SingleOutputStreamOperator<String> asyncIoDataStream = AsyncDataStream.unorderedWait(datasource, new AsyncIoFunction(),
                5000, TimeUnit.MICROSECONDS, 100);

        asyncIoDataStream.print();

        env.execute();
    }
}

class AsyncIoFunction extends RichAsyncFunction<String, String> {

    private transient FutureTask<InputStream> futureTask;

    @Override
    public void open(Configuration parameters) throws Exception {
        Socket hadoop = new Socket("hadoop", 9999);
        futureTask = new FutureTask<InputStream>(hadoop::getInputStream);
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    // 外部数据
                    return IOUtils.toString(futureTask.get());
                } catch (InterruptedException | ExecutionException | IOException e) {
                    // 显示地处理异常。
                    return null;
                }
            }
        }).thenAccept((String externalData) -> resultFuture.complete(Collections.singleton(input + ":" + externalData)));
    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        System.out.println("timeout record is " + input);
    }
}