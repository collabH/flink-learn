package dev.learn.flink.sqlconnector.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @fileName: CustomSourceOptions.java
 * @description: custom source options
 * @author: huangshimin
 * @date: 2023/2/17 17:20
 */
public class CustomSourceOptions {
    public static final ConfigOption<String> PATH;

    static {
        PATH = ConfigOptions.key("custom.path")
                .stringType()
                .noDefaultValue().withDescription("读取数据路径");
    }
}
