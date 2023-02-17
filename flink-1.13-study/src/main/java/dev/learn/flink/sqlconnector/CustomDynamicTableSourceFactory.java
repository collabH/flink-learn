package dev.learn.flink.sqlconnector;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static dev.learn.flink.sqlconnector.options.CustomSourceOptions.PATH;

/**
 * @fileName: CustomDynamicTableSourceFactory.java
 * @description: 自定义DynamicTableSourceFactory
 * @author: huangshimin
 * @date: 2023/2/13 19:37
 */
public class CustomDynamicTableSourceFactory implements DynamicTableSourceFactory {

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(FactoryUtil.FORMAT);
    }

    @Override
    public String factoryIdentifier() {
        return "custom";
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        // 校验table参数
        helper.validate();
        ReadableConfig configuration = helper.getOptions();
        String path = configuration.get(PATH);
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new CustomDynamicTableSource(decodingFormat, resolvedSchema,
                path);
    }


}
