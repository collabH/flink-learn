package dev.learn.flink.tablesql.udfsource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.Set;

/**
 * @fileName: UDFTableSinkFactory.java
 * @description: UDFTableSinkFactory.java类说明
 * @author: by echo huang
 * @date: 2021/1/16 11:35 下午
 */
public class UDFTableSinkFactory implements DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
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
