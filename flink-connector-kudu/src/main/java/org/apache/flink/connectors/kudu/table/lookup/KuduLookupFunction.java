package org.apache.flink.connectors.kudu.table.lookup;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.connectors.kudu.metrics.IntegerGauge;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @fileName: KuduLookupFunction.java
 * @description: 支持kudu lookup function
 * @author: by echo huang
 * @date: 2020/12/29 2:22 下午
 */
public class KuduLookupFunction extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(KuduLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final KuduTableInfo tableInfo;
    private final KuduReaderConfig kuduReaderConfig;
    private final String[] keyNames;
    private final String[] projectedFields;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    private transient Cache<Row, List<Row>> cache;
    private transient KuduReader kuduReader;
    private transient Integer keyCount = 0;

    private KuduLookupFunction(String[] keyNames, KuduTableInfo tableInfo, KuduReaderConfig kuduReaderConfig, String[] projectedFields, KuduLookupOptions kuduLookupOptions) {
        this.tableInfo = tableInfo;
        this.projectedFields = projectedFields;
        this.keyNames = keyNames;
        this.kuduReaderConfig = kuduReaderConfig;
        this.cacheMaxSize = kuduLookupOptions.getCacheMaxSize();
        this.cacheExpireMs = kuduLookupOptions.getCacheExpireMs();
        this.maxRetryTimes = kuduLookupOptions.getMaxRetryTimes();
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param keys keys
     */
    public void eval(Object... keys) {
        if (keys.length != keyNames.length) {
            throw new RuntimeException("lookUpKey和lookUpKeyVals长度不一致");
        }
        ConcurrentMap<Row, List<Row>> cacheMap = this.cache.asMap();
        this.keyCount = cacheMap.size();
        // 用于底层缓存用
        Row keyRow = Row.of(keys);
        if (this.cache != null) {
            List<Row> cacheRows = this.cache.getIfPresent(keyRow);
            if (CollectionUtils.isNotEmpty(cacheRows)) {
                for (Row cacheRow : cacheRows) {
                    collect(cacheRow);
                }
                return;
            }
        }

        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                List<KuduFilterInfo> kuduFilterInfos = buildKuduFilterInfo(keys);
                this.kuduReader.setTableFilters(kuduFilterInfos);
                this.kuduReader.setTableProjections(ArrayUtils.isNotEmpty(projectedFields) ? Arrays.asList(projectedFields) : null);

                KuduInputSplit[] inputSplits = kuduReader.createInputSplits(1);
                ArrayList<Row> rows = new ArrayList<>();
                for (KuduInputSplit inputSplit : inputSplits) {
                    KuduReaderIterator scanner = kuduReader.scanner(inputSplit.getScanToken());
                    // 没有启用cache
                    if (cache == null) {
                        while (scanner.hasNext()) {
                            collect(scanner.next());
                        }
                    } else {
                        while (scanner.hasNext()) {
                            Row row = scanner.next();
                            rows.add(row);
                            collect(row);
                        }
                        rows.trimToSize();
                    }
                }
                cache.put(keyRow, rows);
                break;
            } catch (Exception e) {
                LOG.error(String.format("Kudu scan error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of Kudu scan failed.", e);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    /**
     * build kuduFilterInfo
     *
     * @return
     */
    private List<KuduFilterInfo> buildKuduFilterInfo(Object... keyValS) {
        List<KuduFilterInfo> kuduFilterInfos = Lists.newArrayList();
        for (int i = 0; i < keyNames.length; i++) {
            KuduFilterInfo kuduFilterInfo = KuduFilterInfo.Builder.create(keyNames[i])
                    .equalTo(keyValS[i]).build();
            kuduFilterInfos.add(kuduFilterInfo);
        }
        return kuduFilterInfos;
    }


    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        try {
            super.open(context);
            context.getMetricGroup().addGroup("kudu.lookup").gauge("keys", new IntegerGauge(this.keyCount));
            this.kuduReader = new KuduReader(this.tableInfo, this.kuduReaderConfig);
            // 构造缓存用于缓存kudu数据
            this.cache = this.cacheMaxSize == -1 || this.cacheExpireMs == -1 ? null : Caffeine.newBuilder()
                    .expireAfterWrite(this.cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(this.cacheMaxSize)
                    .build();
        } catch (Exception ioe) {
            LOG.error("Exception while creating connection to Kudu.", ioe);
            throw new RuntimeException("Cannot create connection to Kudu.", ioe);
        }
        LOG.info("end open.");
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        if (null != this.kuduReader) {
            try {
                this.kuduReader.close();
                this.cache.cleanUp();
                // help gc
                this.cache = null;
                this.kuduReader = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("exception when close table", e);
            }
        }
        LOG.info("end close.");
    }

    public static class Builder {
        private KuduTableInfo tableInfo;
        private KuduReaderConfig kuduReaderConfig;
        private String[] keyNames;
        private String[] projectedFields;
        private KuduLookupOptions kuduLookupOptions;

        public static Builder options() {
            return new Builder();
        }

        public Builder tableInfo(KuduTableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public Builder kuduReaderConfig(KuduReaderConfig kuduReaderConfig) {
            this.kuduReaderConfig = kuduReaderConfig;
            return this;
        }

        public Builder keyNames(String[] keyNames) {
            this.keyNames = keyNames;
            return this;
        }

        public Builder projectedFields(String[] projectedFields) {
            this.projectedFields = projectedFields;
            return this;
        }

        public Builder kuduLookupOptions(KuduLookupOptions kuduLookupOptions) {
            this.kuduLookupOptions = kuduLookupOptions;
            return this;
        }

        public KuduLookupFunction build() {
            return new KuduLookupFunction(keyNames, tableInfo, kuduReaderConfig, projectedFields, kuduLookupOptions);
        }
    }
}
