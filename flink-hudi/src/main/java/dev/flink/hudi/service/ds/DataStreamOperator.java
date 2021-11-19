package dev.flink.hudi.service.ds;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hudi.source.StreamReadMonitoringFunction;

/**
 * @fileName: DataStreamOperator.java
 * @description: dataStream操作符
 * @author: huangshimin
 * @date: 2021/11/18 8:09 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DataStreamOperator {
    private StreamReadMonitoringFunction streamReadMonitoringFunction;


    public void checkParams() {
        Preconditions.checkNotNull(streamReadMonitoringFunction, "streamReadMonitoringFunction不能为空!");
    }
}
