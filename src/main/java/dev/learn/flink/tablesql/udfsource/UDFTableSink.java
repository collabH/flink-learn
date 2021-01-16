package dev.learn.flink.tablesql.udfsource;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * @fileName: UDFTableSink.java
 * @description: UDFTableSink.java类说明
 * @author: by echo huang
 * @date: 2021/1/16 11:35 下午
 */
public class UDFTableSink implements DynamicTableSink {
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return null;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return null;
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
