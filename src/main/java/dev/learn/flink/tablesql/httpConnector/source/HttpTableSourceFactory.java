package dev.learn.flink.tablesql.httpConnector.source;

import com.google.common.collect.Sets;
import dev.learn.flink.tablesql.httpConnector.options.ConnectionOptions;
import dev.learn.flink.tablesql.httpConnector.options.HttpClientOptions;
import dev.learn.flink.tablesql.httpConnector.options.RequestParamOptions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Set;

/**
 * @fileName: UDFTableSourceFactory.java
 * @description: UDFTableSourceFactory.java类说明
 * @author: by echo huang
 * @date: 2021/1/16 11:34 下午
 */
public class HttpTableSourceFactory implements DynamicTableSourceFactory {

    /**
     * Request Config
     */
    private static final ConfigOption<String> HTTP_CLIENT_HEADERS =
            ConfigOptions.key("http.client.headers")
                    .stringType()
                    .noDefaultValue();
    private static final ConfigOption<String> HTTP_CLIENT_REQUEST_URL =
            ConfigOptions.key("http.client.request-url")
                    .stringType()
                    .noDefaultValue();
    private static final ConfigOption<String> HTTP_CLIENT_REQUEST_TYPE =
            ConfigOptions.key("http.client.request-type")
                    .stringType()
                    .defaultValue(RequestParamOptions.RequestType.GET.name());
    /**
     * Http Client Params
     */
    private static final ConfigOption<Long> HTTP_CLIENT_HEART_INTERVAL = ConfigOptions
            .key("http.client.heart-interval")
            .longType()
            .defaultValue(30 * 1000L);
    private static final ConfigOption<Long> HTTP_CLIENT_READ_TIMEOUT = ConfigOptions
            .key("http.client.read-timeout")
            .longType()
            .defaultValue(60 * 1000L);

    private static final ConfigOption<Long> HTTP_CLIENT_WRITE_TIMEOUT = ConfigOptions
            .key("http.client.write-timeout")
            .longType()
            .defaultValue(60 * 1000L);

    /**
     * Http Client Connection Pool Params
     */
    private static final ConfigOption<Integer> CONNECTION_POOL_MAX_IDLES = ConfigOptions
            .key("http.client.connection-pool.max.idles")
            .intType()
            .defaultValue(5);

    private static final ConfigOption<Long> CONNECTION_POOL_KEEP_ALIVE = ConfigOptions
            .key("http.client.connection-pool.keep-alive")
            .longType()
            .defaultValue(5 * 60 * 1000L);

    private static final ConfigOption<Long> CONNECTION_TIMEOUT = ConfigOptions
            .key("http.client.connection.timeout")
            .longType()
            .defaultValue(60 * 1000L);

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 发现实现的DeserializationFormatFactory
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        // 校验table参数
        helper.validate();
        // 获取table option
        ReadableConfig options = helper.getOptions();
        String requestUrl = options.get(HTTP_CLIENT_REQUEST_URL);
        String headers = options.get(HTTP_CLIENT_HEADERS);
        String requestType = options.get(HTTP_CLIENT_REQUEST_TYPE);
        Long heartInterval = options.get(HTTP_CLIENT_HEART_INTERVAL);
        Long writeTimeout = options.get(HTTP_CLIENT_WRITE_TIMEOUT);
        Long readTimeout = options.get(HTTP_CLIENT_READ_TIMEOUT);
        Long keepAlive = options.get(CONNECTION_POOL_KEEP_ALIVE);
        Integer maxIdes = options.get(CONNECTION_POOL_MAX_IDLES);
        Long connectionTimeout = options.get(CONNECTION_TIMEOUT);
        RequestParamOptions requestParamOptions = RequestParamOptions.builder().requestUrl(requestUrl)
                .headers(headers)
                .requestType(requestType)
                .build();
        ConnectionOptions connectionOptions = ConnectionOptions.builder().connectionTimeout(connectionTimeout)
                .keepAliveDuration(keepAlive)
                .maxIdleConnections(maxIdes).build();
        HttpClientOptions httpClientOptions = HttpClientOptions.builder()
                .heartInterval(heartInterval)
                .readTimeout(readTimeout)
                .writeTimeout(writeTimeout).build();

        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return new HttpTableSource(decodingFormat, requestParamOptions, connectionOptions, httpClientOptions,
                producedDataType);
    }

    @Override
    public String factoryIdentifier() {
        return "http";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(FactoryUtil.FORMAT,
                HTTP_CLIENT_REQUEST_URL);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(HTTP_CLIENT_HEADERS, HTTP_CLIENT_WRITE_TIMEOUT, HTTP_CLIENT_READ_TIMEOUT, HTTP_CLIENT_HEART_INTERVAL,
                CONNECTION_POOL_KEEP_ALIVE, CONNECTION_POOL_MAX_IDLES, CONNECTION_TIMEOUT, HTTP_CLIENT_REQUEST_TYPE);
    }
}
