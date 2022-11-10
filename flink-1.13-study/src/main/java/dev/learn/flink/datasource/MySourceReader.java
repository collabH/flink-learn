package dev.learn.flink.datasource;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import java.util.Map;
import java.util.function.Supplier;

/**
 * @fileName: MySource.java
 * @description: MySource.java类说明
 * @author: huangshimin
 * @date: 2022/11/10 11:38 AM
 */
public class MySourceReader<E, T, SplitT extends MySplit, SplitStateT> extends
        SingleThreadMultiplexSourceReaderBase<E, T, SplitT, SplitStateT> {
    public MySourceReader(Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
                          RecordEmitter<E, T, SplitStateT> recordEmitter, Configuration config,
                          SourceReaderContext context) {
        super(splitReaderSupplier, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, SplitStateT> map) {

    }

    @Override
    protected SplitStateT initializedState(SplitT splitT) {
        return null;
    }

    @Override
    protected SplitT toSplitType(String s, SplitStateT splitStateT) {
        return null;
    }
}
