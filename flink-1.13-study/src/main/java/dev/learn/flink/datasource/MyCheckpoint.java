package dev.learn.flink.datasource;

import org.apache.flink.runtime.checkpoint.Checkpoint;

/**
 * @fileName: MyCheckpoint.java
 * @description: MyCheckpoint.java类说明
 * @author: huangshimin
 * @date: 2022/11/10 11:29 AM
 */
public class MyCheckpoint implements Checkpoint {
    @Override
    public long getCheckpointID() {
        return 0;
    }

    @Override
    public void discard() throws Exception {

    }
}
