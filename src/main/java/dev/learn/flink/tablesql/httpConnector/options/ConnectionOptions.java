package dev.learn.flink.tablesql.httpConnector.options;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

/**
 * @fileName: ConnectionPoolConfig.java
 * @description: ConnectionPoolConfig.java类说明
 * @author: by echo huang
 * @date: 2021/1/17 5:59 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConnectionOptions {
    private int maxIdleConnections;
    private long keepAliveDuration;
    private long connectionTimeout;
}
