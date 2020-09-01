package dev.learn.flink.domain;

/**
 * @fileName: MessageEvent.java
 * @description: MessageEvent.java类说明
 * @author: by echo huang
 * @date: 2020/9/1 9:56 下午
 */
public class MessageEvent {
    private String id;
    private String timestamp;
    private String temperature;

    public MessageEvent() {
    }

    public MessageEvent(String id, String timestamp, String temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTemperature() {
        return temperature;
    }

    public void setTemperature(String temperature) {
        this.temperature = temperature;
    }
}
