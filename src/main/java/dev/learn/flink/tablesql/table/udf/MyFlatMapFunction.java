package dev.learn.flink.tablesql.table.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * @fileName: MyFlatMapFunction.java
 * @description: MyFlatMapFunction.java类说明
 * @author: by echo huang
 * @date: 2020/9/5 1:18 下午
 */
public class MyFlatMapFunction extends TableFunction<Integer> {
    public void eval(Integer num) {
        collect(num + 1);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        typeFactory.createDataType(Integer.class);
        return super.getTypeInference(typeFactory);
    }
}
