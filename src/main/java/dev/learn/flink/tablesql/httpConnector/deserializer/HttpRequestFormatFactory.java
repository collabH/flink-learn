package dev.learn.flink.tablesql.httpConnector.deserializer;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

/**
 * @fileName: HttpRequestFormatFactory.java
 * @description: HttpRequestFormatFactory.java类说明
 * @author: by echo huang
 * @date: 2021/1/17 7:02 下午
 */
public class HttpRequestFormatFactory implements DeserializationFormatFactory {
    private static final ConfigOption<String> HTTP_CLIENT_FORMAT_CLASSNAME =
            ConfigOptions.key("http.client.format.classname")
                    .stringType()
                    .noDefaultValue();

    private static final ConfigOption<Boolean> HTTP_CLIENT_IS_ARRAY =
            ConfigOptions.key("http.client.is.array")
                    .booleanType()
                    .defaultValue(true);

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
        FactoryUtil.validateFactoryOptions(this, readableConfig);
        String formatClassName = readableConfig.get(HTTP_CLIENT_FORMAT_CLASSNAME);
        Boolean isArray = readableConfig.get(HTTP_CLIENT_IS_ARRAY);
        return new HttpJsonBeanFormat(formatClassName, isArray);
    }

    @Override
    public String factoryIdentifier() {
        return "http-json-bean";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(HTTP_CLIENT_FORMAT_CLASSNAME);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(HTTP_CLIENT_IS_ARRAY);
    }
}
