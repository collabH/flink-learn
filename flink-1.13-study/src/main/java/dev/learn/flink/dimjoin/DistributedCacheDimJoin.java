package dev.learn.flink.dimjoin;

import com.google.common.collect.Maps;
import dev.learn.flink.FlinkEnvUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

/**
 * @fileName: DistributedCacheDimJoin.java
 * @description: 分布式缓存dim join
 * @author: huangshimin
 * @date: 2022/12/6 2:49 PM
 */
public class DistributedCacheDimJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        // 注册分布式cache文件
        env.registerCachedFile("/Users/huangshimin/Documents/study/flink-learn/flink-1.13-study/data/users.csv",
                "users");
        env.fromElements(1, 2, 3)
                .map(new RichMapFunction<Integer, String>() {
                    private final Map<Integer, String> distributedCacheMap = Maps.newHashMap();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 解析加载distributedCache文件
                        super.open(parameters);
                        DistributedCache distributedCache = getRuntimeContext().getDistributedCache();
                        File userFile = distributedCache.getFile("users");
                        CSVFormat csvFormat = CSVFormat.DEFAULT.withRecordSeparator(",");
                        FileReader fileReader = new FileReader(userFile);
                        //创建CSVParser对象
                        CSVParser parser = new CSVParser(fileReader, csvFormat);
                        List<CSVRecord> records = parser.getRecords();
                        for (CSVRecord record : records) {
                            // 放入内存cache中
                            distributedCacheMap.put(Integer.parseInt(record.get(0)), record.get(1));
                        }
                    }

                    @Override
                    public String map(Integer key) throws Exception {
                        String name = distributedCacheMap.getOrDefault(key, "NA");
                        return key + ":" + name;
                    }
                }).print("data");
        env.execute();
    }
}
