package dev.learn.flink.sqlconnector;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.table.catalog.Column;

import java.io.Serializable;
import java.util.List;

/**
 * @fileName: ColumnHolder.java
 * @description: ColumnHolder.java类说明
 * @author: huangshimin
 * @date: 2023/2/17 19:27
 */
@Data
@AllArgsConstructor
public class ColumnHolder implements Serializable {
    private List<Column> columns;
}
