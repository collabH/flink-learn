package dev.flink.hudi.service.ds;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hudi.table.HoodieTableSource;

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
    private HoodieTableSource hoodieTableSource;

    public void checkParams() {
        Preconditions.checkNotNull(hoodieTableSource, "hoodieTableSource不能为空!");
    }
}
