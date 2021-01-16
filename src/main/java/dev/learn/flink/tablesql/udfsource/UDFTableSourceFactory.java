package dev.learn.flink.tablesql.udfsource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.Set;

/**
 * @fileName: UDFTableSourceFactory.java
 * @description: UDFTableSourceFactory.java类说明
 * @author: by echo huang
 * @date: 2021/1/16 11:34 下午
 */
public class UDFTableSourceFactory implements DynamicTableSourceFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }
}
