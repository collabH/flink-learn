package dev.learn.flink.datasource;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.function.Supplier;

public class FixedFetcherSizeSourceReader<E, T, SplitT extends SourceSplit, SplitStateT>
        extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    public FixedFetcherSizeSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitFetcherSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                new FixedSizeSplitFetcherManager<>(
                        config.getInteger(ConfigOptions.key("fetcher_nums").intType().noDefaultValue()),
                        elementsQueue,
                        splitFetcherSupplier),
                recordEmitter,
                config,
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, SplitStateT> finishedSplitIds) {
        // 在回调过程中对完成的分片进行处理。
    }

    @Override
    protected SplitStateT initializedState(SplitT split) {
        return null;
    }

    @Override
    protected SplitT toSplitType(String splitId, SplitStateT splitState) {
        return null;
    }
}