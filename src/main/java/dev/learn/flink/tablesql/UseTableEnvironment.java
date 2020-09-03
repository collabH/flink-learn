package dev.learn.flink.tablesql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: UseTableEnvironment.java
 * @description: UseTableEnvironment.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 11:25 上午
 */
public class UseTableEnvironment {
    public static void main(String[] args) {
        // create stream execute environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);

        // create batch execute environment
        EnvironmentSettings batchSettings= EnvironmentSettings.newInstance()
                .inBatchMode().useBlinkPlanner().build();
        TableEnvironment.create(batchSettings);


    }
}
