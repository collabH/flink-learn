package dev.hudi.client.write;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.util.List;

/**
 * @fileName: UseWriteClient.java
 * @description: 使用java writeClient
 * @author: huangshimin
 * @date: 2021/11/26 3:40 下午
 */
public class UseWriteClient {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.addResource(new Path("/Users/huangshimin/Documents/dev/soft/hadoop330/etc/hadoop/hdfs-site" +
                ".xml"));
        configuration.addResource(new Path("/Users/huangshimin/Documents/dev/soft/hadoop330/etc/hadoop/core-site" +
                ".xml"));
        HoodieJavaEngineContext hoodieJavaEngineContext = new HoodieJavaEngineContext(configuration);
        HoodieWriteConfig hoodieWriteConfig = new HoodieWriteConfig.Builder()
                .forTable("java_hudi_table")
                .withAutoCommit(true)
                .withEmbeddedTimelineServerEnabled(true)
                .withEmbeddedTimelineServerPort(9999)
                .withEmbeddedTimelineServerReuseEnabled(true)
                .withPreCombineField("update_time")
                .withPath("hdfs://hadoop:8020/user/java/java_hudi_table")
                .withEngineType(EngineType.JAVA)
                .withIndexConfig(new HoodieIndexConfig.Builder()
                        .withIndexType(HoodieIndex.IndexType.BLOOM)
                        .withEngineType(EngineType.JAVA).build())
                .withClusteringConfig(HoodieClusteringConfig.newBuilder()
                        .withInlineClustering(true)
                        .withAsyncClustering(true)
                        .withClusteringSortColumns("test").build())
                .build();
        HoodieJavaWriteClient<DefaultHoodieRecordPayload> hoodieJavaWriteClient =
                new HoodieJavaWriteClient<DefaultHoodieRecordPayload>(hoodieJavaEngineContext, hoodieWriteConfig);
        List<HoodieRecord<DefaultHoodieRecordPayload>> hoodieRecords =
                Lists.newArrayList();
        HoodieRecord<DefaultHoodieRecordPayload> record =
                new HoodieRecord<DefaultHoodieRecordPayload>();
        hoodieRecords.add(record);
        hoodieJavaWriteClient.insert(hoodieRecords, System.currentTimeMillis() + "");

    }
}
