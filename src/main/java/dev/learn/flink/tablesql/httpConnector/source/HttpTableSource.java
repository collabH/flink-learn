package dev.learn.flink.tablesql.httpConnector.source;

import dev.learn.flink.tablesql.httpConnector.options.ConnectionOptions;
import dev.learn.flink.tablesql.httpConnector.options.HttpClientOptions;
import dev.learn.flink.tablesql.httpConnector.options.RequestParamOptions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * @fileName: UDFTableSource.java
 * @description: UDFTableSource.java类说明
 * @author: by echo huang
 * @date: 2021/1/16 4:12 下午
 */
public class HttpTableSource implements ScanTableSource {
    private final RequestParamOptions requestParamOptions;
    private final ConnectionOptions connectionOptions;
    private final HttpClientOptions httpClientOptions;
    private final DataType producedDataType;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public HttpTableSource(DecodingFormat<DeserializationSchema<RowData>> decodingFormat, RequestParamOptions requestParamOptions, ConnectionOptions connectionOptions, HttpClientOptions httpClientOptions, DataType producedDataType) {
        this.decodingFormat = decodingFormat;
        this.requestParamOptions = requestParamOptions;
        this.connectionOptions = connectionOptions;
        this.httpClientOptions = httpClientOptions;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpTableSource(decodingFormat, requestParamOptions, connectionOptions, httpClientOptions, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Http Table Source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // 获取序列化的schema
        DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(scanContext, producedDataType);

        HttpSourceFunction sourceFunction = new HttpSourceFunction(requestParamOptions, connectionOptions, httpClientOptions, deserializer);

        return SourceFunctionProvider.of(sourceFunction, false);
    }
}
