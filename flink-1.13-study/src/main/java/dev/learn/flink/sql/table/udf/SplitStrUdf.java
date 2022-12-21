package dev.learn.flink.sql.table.udf;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * @fileName: SplitStrUdf.java
 * @description: SplitStrUdf.java类说明
 * @author: huangshimin
 * @date: 2022/12/20 8:08 PM
 */
public class SplitStrUdf extends UserDefinedFunction {
    @Override
    public FunctionKind getKind() {
        return FunctionKind.SCALAR;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory dataTypeFactory) {
        return null;
    }
}
