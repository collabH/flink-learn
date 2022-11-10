package dev.flink.hudi.service.ds;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;

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
    private String targetTable;
    private List<String> columns;
    private List<String> pk;
    private List<String> partition;
    private Map<String, String> tableOptions;


    public void checkParams() {
        Preconditions.checkNotNull(targetTable, "targetTable不能为空!");
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(pk), "pk不能为空!");
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(partition), "partition不能为空!");
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(columns), "columns不能为空!");
        Preconditions.checkArgument(MapUtils.isNotEmpty(tableOptions), "tableOptions不能为空!");
    }
}
