package dev.learn.metrics;

import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Properties;

/**
 * @fileName: MetricReporterFactory.java
 * @description: MetricReporterFactory.java类说明
 * @author: by echo huang
 * @date: 2021/7/3 5:48 下午
 */
public class MetricReporterFactoryDemo {
    public static void main(String[] args) {
        MetricReporterFactory metricReporterFactory = new MetricReporterFactory() {
            @Override
            public MetricReporter createMetricReporter(Properties properties) {
                return new MetricReporterImpl();
            }
        };

        MetricReporter metricReporter = metricReporterFactory.createMetricReporter(new Properties());
//        metricReporter.notifyOfAddedMetric();
    }
}
