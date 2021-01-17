package dev.learn.flink.tablesql.httpConnector.source;

import com.google.common.base.Preconditions;
import dev.learn.flink.tablesql.httpConnector.options.ConnectionOptions;
import dev.learn.flink.tablesql.httpConnector.options.HttpClientOptions;
import dev.learn.flink.tablesql.httpConnector.options.RequestParamOptions;
import dev.learn.utils.http.OkHttpClientUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

/**
 * @fileName: HttpSourceFunction.java
 * @description: HttpSourceFunction.java类说明
 * @author: by echo huang
 * @date: 2021/1/17 11:44 下午
 */
public class HttpSourceFunction extends RichSourceFunction<RowData> {
    private final RequestParamOptions requestParamOptions;
    private final ConnectionOptions connectionOptions;
    private final HttpClientOptions httpClientOptions;
    private final DeserializationSchema<RowData> deserializer;
    private transient OkHttpClient httpClient;

    public HttpSourceFunction(RequestParamOptions requestParamOptions, ConnectionOptions connectionOptions, HttpClientOptions httpClientOptions, DeserializationSchema<RowData> deserializer) {
        this.requestParamOptions = requestParamOptions;
        this.connectionOptions = connectionOptions;
        this.httpClientOptions = httpClientOptions;
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        OkHttpClientUtils okHttpClientUtils = new OkHttpClientUtils();
        this.httpClient = okHttpClientUtils.initialHttpClient(httpClientOptions, connectionOptions, false);
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        String headers = requestParamOptions.getHeaders();
        String requestType = requestParamOptions.getRequestType();
        String[] headersArr = headers.split(",");
        if (ArrayUtils.isNotEmpty(headersArr)) {
            for (String headerKv : headersArr) {
                String[] headerKvArr = headerKv.split(":");
                Preconditions.checkArgument(ArrayUtils.isNotEmpty(headerKvArr) && headerKv.length() == 2, "header参数异常");
                switch (RequestParamOptions.RequestType.valueOf(requestType)) {
                    case GET:
                        httpClient.newCall(new Request().newBuilder().addHeader(headerKvArr[0],headerKvArr[1]));
                    case PATCH:
                    case POST:
                    case DELETE:
                }
            }
        }
    }


    @Override
    public void cancel() {

    }
}
