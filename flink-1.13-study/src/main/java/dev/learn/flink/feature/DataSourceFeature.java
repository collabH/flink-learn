package dev.learn.flink.feature;

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
    static class CustomSplit{

    }

    /**
     * 自定义sourceReader
     * 会请求分片并进行处理，例如读取分片所表示的文件或日志分区。SourceReader 在 TaskManagers 上的 SourceOperators 并行运行，并产生并行的事件流/记录流。
     */
    static class CustomSourceReader{

    }

    /**
     * 自定义SplitEnumerator
     * 会生成分片并将它们分配给 SourceReader。该组件在 JobManager 上以单并行度运行，负责对未分配的分片进行维护，并以均衡的方式将其分配给 reader。
     */
    static class CustomSplitEnumerator{

    }
}
