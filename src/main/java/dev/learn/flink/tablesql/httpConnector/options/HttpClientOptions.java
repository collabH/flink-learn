package dev.learn.flink.tablesql.httpConnector.options;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @fileName: HttpClientConfig.java
 * @description: HttpClientConfig.java类说明
 * @author: by echo huang
 * @date: 2021/1/17 5:59 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class HttpClientOptions {
    private Long heartInterval;
    private Long readTimeout ;
    private Long writeTimeout;
}
