package dev.learn.flink.feature;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @fileName: DataSourceFeature.java
 * @description: DataSourceFeature.java类说明
 * @author: huangshimin
 * @date: 2021/8/22 10:25 下午
 */
public class DataSourceFeature {


    /**
     * 自定义分片器
     * 对一部分 source 数据的包装，如一个文件或者日志分区。分片是 source 进行任务分配和数据并行读取的基本粒度。
     */
    static class CustomSplit {

    }

    /**
     * 自定义sourceReader
     * 会请求分片并进行处理，例如读取分片所表示的文件或日志分区。SourceReader 在 TaskManagers 上的 SourceOperators 并行运行，并产生并行的事件流/记录流。
     */
    static class CustomSourceReader extends SourceReaderBase {

        public CustomSourceReader(FutureCompletingBlockingQueue elementsQueue, SplitFetcherManager splitFetcherManager, RecordEmitter recordEmitter, Configuration config, SourceReaderContext context) {
            super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        }

        @Override
        protected void onSplitFinished(Map map) {

        }

        @Override
        protected Object initializedState(SourceSplit sourceSplit) {
            return null;
        }

        @Override
        protected SourceSplit toSplitType(String s, Object o) {
            return null;
        }
    }

    /**
     * 自定义SplitEnumerator
     * 会生成分片并将它们分配给 SourceReader。该组件在 JobManager 上以单并行度运行，负责对未分配的分片进行维护，并以均衡的方式将其分配给 reader。
     * 1. sourceReader的注册处理
     * 2. sourceReader的失败处理
     * * SourceReader 失败时会调用 addSplitsBack() 方法。SplitEnumerator应当收回已经被分配，但尚未被该 SourceReader 确认（acknowledged）的分片。
     * 3. sourceEvent的处理
     * * SourceEvents 是 SplitEnumerator 和 SourceReader 之间来回传递的自定义事件。可以利用此机制来执行复杂的协调任务。
     * 4. 分片的发现以及分配
     * * SplitEnumerator 可以将分片分配到 SourceReader 从而响应各种事件，包括发现新的分片，新 SourceReader 的注册，SourceReader 的失败处理等
     */
    static class CustomSplitEnumerator implements SplitEnumerator<MySplitSource, Long> {


        @Override
        public void start() {
//            SourceCoordinatorContext<MySplitSource> enumContext=new SourceCoordinatorContext<>();
//             enumContext.callAsync(this::discoverSplits, splits -> {
//            Map<Integer, List<MockSourceSplit>> assignments = new HashMap<>();
//            int parallelism = enumContext.currentParallelism();
//            for (MockSourceSplit split : splits) {
//                int owner = split.splitId().hashCode() % parallelism;
//                assignments.computeIfAbsent(owner, new ArrayList<>()).add(split);
//            }
//            enumContext.assignSplits(new SplitsAssignment<>(assignments));
//        }, 0L, DISCOVER_INTERVAL);
        }

        @Override
        public void handleSplitRequest(int i, @Nullable String s) {

        }

        @Override
        public void addSplitsBack(List<MySplitSource> list, int i) {

        }

        @Override
        public void addReader(int i) {

        }

        @Override
        public Long snapshotState(long l) throws Exception {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class MySplitSource implements SourceSplit {

        @Override
        public String splitId() {
            return null;
        }
    }
}
