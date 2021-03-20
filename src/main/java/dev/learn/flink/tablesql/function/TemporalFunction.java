package dev.learn.flink.tablesql.function;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @fileName: TemporalFunction.java
 * @description: TemporalFunction.java类说明
 * @author: by echo huang
 * @date: 2021/3/20 4:28 下午
 */
public class TemporalFunction {
    public static void main(String[] args) {
        // 获取 stream 和 table 环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamEnvironment.getEnv(executionEnvironment);

// 提供一个汇率历史记录表静态数据集
        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

// 用上面的数据集创建并注册一个示例表
// 在实际设置中，应使用自己的表替换它
        DataStream<Tuple2<String, Long>> ratesHistoryStream = executionEnvironment.fromCollection(ratesHistoryData);
        Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, $("r_currency"), $("r_rate"), $("r_proctime").proctime());

        tEnv.createTemporaryView("RatesHistory", ratesHistory);

// 创建和注册时态表函数
// 指定 "r_proctime" 为时间属性，指定 "r_currency" 为主键
        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction($("r_proctime"), $("r_currency")); // <==== (1)

        /**
         * 时态表
         */
        tEnv.createTemporaryFunction("rates", rates);

        ratesHistory.as("rh").joinLateral(call(rates,$("r_proctime")).as("r"), $("r_currency"))
                .select($("rh.*"))
                .execute()
                .print();
    }
}