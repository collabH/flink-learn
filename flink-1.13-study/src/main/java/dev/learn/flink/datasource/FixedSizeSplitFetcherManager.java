package dev.learn.flink.datasource;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class FixedSizeSplitFetcherManager<E, SplitT extends SourceSplit>
        extends SplitFetcherManager<E, SplitT> {
    private final int numFetchers;

    public FixedSizeSplitFetcherManager(
            int numFetchers,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
        this.numFetchers = numFetchers;
        // 创建 numFetchers 个分片提取器.
        for (int i = 0; i < numFetchers; i++) {
            startFetcher(createSplitFetcher());
        }
    }

    @Override
    public void addSplits(List<SplitT> splitsToAdd) {
        // 根据它们所属的提取器将分片聚集在一起。
        Map<Integer, List<SplitT>> splitsByFetcherIndex = new HashMap<>();
        splitsToAdd.forEach(split -> {
            int ownerFetcherIndex = split.hashCode() % numFetchers;
            splitsByFetcherIndex
                    .computeIfAbsent(ownerFetcherIndex, s -> new ArrayList<>())
                    .add(split);
        });
        // 将分片分配给它们所属的提取器。
        splitsByFetcherIndex.forEach((fetcherIndex, splitsForFetcher) -> {
            fetchers.get(fetcherIndex).addSplits(splitsForFetcher);
        });
    }
}