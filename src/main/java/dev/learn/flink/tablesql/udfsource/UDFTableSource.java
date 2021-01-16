package dev.learn.flink.tablesql.udfsource;

import org.apache.flink.table.connector.source.DynamicTableSource;

/**
 * @fileName: UDFTableSource.java
 * @description: UDFTableSource.java类说明
 * @author: by echo huang
 * @date: 2021/1/16 4:12 下午
 */
public class UDFTableSource implements DynamicTableSource {
    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
