package dev.flink.hudi.builder.column;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @fileName: ColumnInfo.java
 * @description: 列详情信息
 * @author: huangshimin
 * @date: 2021/12/8 8:15 下午
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ColumnInfo {
    private String columnName;
    private String columnType;
}
