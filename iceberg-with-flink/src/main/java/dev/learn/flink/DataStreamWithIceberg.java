package dev.learn.flink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;

import java.util.HashMap;
import java.util.Map;

/**
 * @fileName: DataStreamWithIceberg.java
 * @description: 使用datastream操作iceberg
 * @author: huangshimin
 * @date: 2021/7/31 7:43 下午
 */
public class DataStreamWithIceberg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = FlinkEnv.getStreamEnv();

        Map<String, String> properties = Maps.newHashMap();
        properties.put("warehouse", "hdfs://localhost:8020/user/hive/warehouse");
        properties.put("uri", "thrift://localhost:9083");
        TableLoader tableLoader = TableLoader
                .fromCatalog(CatalogLoader.hive("hive_catalog", new Configuration(), properties),
                        TableIdentifier.of("iceberg_db.test"));
        DataStream<RowData> dataStream = FlinkSource.forRowData()
                .env(streamEnv)
                .tableLoader(tableLoader)
                .streaming(false)
                .build();
        // 流式读取
//        DataStream<RowData> stream = FlinkSource.forRowData()
//                .env(streamEnv)
//                .tableLoader(tableLoader)
//                .streaming(true)
//                .startSnapshotId(3821550127947089987L)
//                .build();
        streamEnv.fromCollection(Lists.newArrayList(
                GenericRowData.of(new Test(1, "hsm")))).print();
//        DataStreamSink<RowData> dataDataStreamSink =
//                FlinkSink.forRowData()
//                        .tableLoader(tableLoader).overwrite(true)
//                        .distributionMode(DistributionMode.HASH)
//                        .writeParallelism(3)
//                        .build();
//        dataDataStreamSink.name("sink iceberg");
//        dataStream.print();

        // 合并小文件
        Table table = tableLoader.loadTable();
        RewriteDataFilesActionResult execute = Actions.forTable(table)
                .rewriteDataFiles()
                .maxParallelism(3)
                .execute();
        streamEnv.execute();
    }
}

class Test {
    private Integer id;
    private String name;

    public Test() {
    }

    public Test(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
