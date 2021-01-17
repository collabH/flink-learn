package dev.learn.flink.tablesql.httpConnector.source;

import com.google.common.base.Preconditions;
import dev.learn.flink.tablesql.httpConnector.options.ConnectionOptions;
import dev.learn.flink.tablesql.httpConnector.options.HttpClientOptions;
import dev.learn.flink.tablesql.httpConnector.options.RequestParamOptions;
import dev.learn.utils.http.OkHttpClientUtils;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import java.util.Objects;

/**
 * @fileName: HttpSourceFunction.java
 * @description: HttpSourceFunction.java类说明
 * @author: by echo huang
 * @date: 2021/1/17 11:44 下午
 */
public class HttpSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    private final RequestParamOptions requestParamOptions;
    private final ConnectionOptions connectionOptions;
    private final HttpClientOptions httpClientOptions;
    private final DeserializationSchema<RowData> deserializer;
    private transient OkHttpClient httpClient;
    private boolean isRunning = true;

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
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (isRunning) {
            String headers = requestParamOptions.getHeaders();
            String requestType = requestParamOptions.getRequestType();
            String[] headersArr = headers.split(",");
            Call request = null;
            if (ArrayUtils.isNotEmpty(headersArr)) {
                for (String headerKv : headersArr) {
                    String[] headerKvArr = headerKv.split(":");
                    Preconditions.checkArgument(ArrayUtils.isNotEmpty(headerKvArr) && headerKv.length() == 2, "header参数异常");

                    switch (RequestParamOptions.RequestType.valueOf(requestType)) {
                        case GET:
                            request = httpClient.newCall(new Request.Builder().get().url(requestParamOptions.getRequestUrl()).addHeader(headerKvArr[0], headerKvArr[1]).build());
                            break;
                        case PATCH:
                            break;
                        case POST:
                            break;
                        case DELETE:
                            break;
                        default:
                            throw new RuntimeException("请求类型不支持");
                    }
                }
                if (null != request) {
                    byte[] result = Objects.requireNonNull(request.execute().body()).bytes();
                    ctx.collect(deserializer.deserialize(result));
                }
            }
        }
    }


    @Override
    public void cancel() {
        isRunning = false;
        httpClient = null;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }
}
