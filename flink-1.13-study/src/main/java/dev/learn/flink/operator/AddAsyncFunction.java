package dev.learn.flink.operator;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

/**
 * @fileName: AsyncFunction.java
 * @description: AsyncFunction.java类说明
 * @author: huangshimin
 * @date: 2022/11/30 5:59 PM
 */
public class AddAsyncFunction extends RichAsyncFunction<Integer, Long> {
    @Override
    public void asyncInvoke(Integer key, ResultFuture<Long> resultFuture) throws Exception {
        // 发送异步请求，接收 future 结果
        final Future<Long> result = new FutureTask<>(() -> key + 1L);

        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture.supplyAsync(new Supplier<Long>() {

            @Override
            public Long get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // 显示地处理异常。
                    return null;
                }
            }
        }).thenAccept((Long dbResult) -> {
            resultFuture.complete(Collections.singleton(dbResult));
        });
    }
}
