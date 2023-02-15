package dev.learn.flink.sqlconnector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * @fileName: CustomDynamicTableSourceFactory.java
 * @description: 自定义DynamicTableSourceFactory
 * @author: huangshimin
 * @date: 2023/2/13 19:37
 */
public class CustomDynamicTableSourceFactory implements DynamicTableSourceFactory {
    private static final ConfigOption<String> PATH;
    private static final ConfigOption<String> FORMAT;

    static {
        PATH = ConfigOptions.key("custom.path")
                .stringType()
                .noDefaultValue().withDescription("读取数据路径");
        FORMAT = ConfigOptions.key("custom.format").stringType()
                .defaultValue("json").withDescription("数据格式，默认json,支持csv、json");
    }


    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FORMAT);
        return options;
    }
    @Override
    public String factoryIdentifier() {
        return "custom";
    }
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig configuration = helper.getOptions();
        String path = configuration.get(PATH);
        String format = configuration.get(FORMAT);
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new CustomDynamicTableSource(resolvedSchema,
                DataFormat.valueOf(format.toUpperCase()), path);
    }



}
