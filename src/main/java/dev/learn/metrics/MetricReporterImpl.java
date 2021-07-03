package dev.learn.metrics;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;

/**
 * @fileName: MetricReporterImpl.java
 * @description: flink监控
 * @author: by echo huang
 * @date: 2021/7/3 5:47 下午
 */
public class MetricReporterImpl implements MetricReporter {
    /**
     * 初始化参数
     * @param metricConfig
     */
    @Override
    public void open(MetricConfig metricConfig) {

    }

    /**
     * 关闭
     */
    @Override
    public void close() {

    }

    /**
     * 添加指标
     * @param metric
     * @param s
     * @param metricGroup
     */
    @Override
    public void notifyOfAddedMetric(Metric metric, String s, MetricGroup metricGroup) {

    }

    /**
     * 移除指标
     * @param metric
     * @param s
     * @param metricGroup
     */
    @Override
    public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {

    }
}
