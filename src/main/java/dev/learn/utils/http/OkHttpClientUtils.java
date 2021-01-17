package dev.learn.utils.http;

import dev.learn.flink.tablesql.httpConnector.options.ConnectionOptions;
import dev.learn.flink.tablesql.httpConnector.options.HttpClientOptions;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @fileName: OkHttpClient.java
 * @description: OkHttpClient.java类说明
 * @author: by echo huang
 * @date: 2021/1/17 5:26 下午
 */
public class OkHttpClientUtils {

    /**
     * 初始化okHttp客户端
     *
     * @param httpClientOptions
     * @param connectionOptions
     * @param isSsl
     * @return
     */
    public OkHttpClient initialHttpClient(HttpClientOptions httpClientOptions,
                                          ConnectionOptions connectionOptions, boolean isSsl) {
        ConnectionPool connectionPool = new ConnectionPool(connectionOptions.getMaxIdleConnections(), connectionOptions.getKeepAliveDuration()
                , MILLISECONDS);
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder()
                .readTimeout(httpClientOptions.getReadTimeout(), MILLISECONDS)
                .writeTimeout(httpClientOptions.getWriteTimeout(), MILLISECONDS)
                .pingInterval(httpClientOptions.getHeartInterval(), MILLISECONDS)
                .connectionPool(connectionPool)
                .connectTimeout(connectionOptions.getConnectionTimeout(), MILLISECONDS);
        return okHttpClientBuilder.build();
    }
}
