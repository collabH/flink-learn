package dev.learn.record.domain;

import lombok.Data;

/**
 * @fileName: LogEvent.java
 * @description: LogEvent.java类说明
 * @author: by echo huang
 * @date: 2020/9/6 8:59 下午
 */
@Data
public class LogEvent {
    private String ip;
    private Long userId;
    private Long eventTime;
    private String method;
    private String url;
}
