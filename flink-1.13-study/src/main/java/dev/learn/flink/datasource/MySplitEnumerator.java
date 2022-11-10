package dev.learn.flink.datasource;

import org.apache.flink.api.connector.source.SplitEnumerator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * @fileName: MySplitEnumerator.java
 * @description: MySplitEnumerator.java类说明
 * @author: huangshimin
 * @date: 2022/11/10 11:28 AM
 */
public class MySplitEnumerator implements SplitEnumerator<MySplit,MyCheckpoint> {
    @Override
    public void start() {
        
    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {

    }

    @Override
    public void addSplitsBack(List<MySplit> list, int i) {

    }

    @Override
    public void addReader(int i) {

    }

    @Override
    public MyCheckpoint snapshotState(long l) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
