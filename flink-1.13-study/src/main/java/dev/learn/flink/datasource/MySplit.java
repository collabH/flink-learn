package dev.learn.flink.datasource;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * @fileName: MySplit.java
 * @description: MySplit.java类说明
 * @author: huangshimin
 * @date: 2022/11/10 11:29 AM
 */
public class MySplit implements SourceSplit {
    @Override
    public String splitId() {
        return null;
    }
}
