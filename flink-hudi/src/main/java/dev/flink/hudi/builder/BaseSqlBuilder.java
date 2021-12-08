package dev.flink.hudi.builder;

import com.google.common.base.Preconditions;
import dev.flink.hudi.builder.column.ColumnInfo;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @fileName: BaseSqlBuilder.java
 * @description: sql构造器基类
 * @author: huangshimin
 * @date: 2021/12/8 7:55 下午
 */
@Data
public abstract class BaseSqlBuilder {
    private Map<String, Object> props;
    private String tableName;
    private List<ColumnInfo> columnInfos;

    public BaseSqlBuilder(Map<String, Object> props, String tableName, List<ColumnInfo> columnInfos) {
        this.props = props;
        this.tableName = tableName;
        this.columnInfos = columnInfos;
    }

    public void valid() {
        Preconditions.checkArgument(MapUtils.isNotEmpty(this.props));
        Preconditions.checkArgument(StringUtils.isNotEmpty(this.tableName));
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(this.columnInfos));
    }

   public abstract String generatorDDL();

    public abstract String generatorDML();
}