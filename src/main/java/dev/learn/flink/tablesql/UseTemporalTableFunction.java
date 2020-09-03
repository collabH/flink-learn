package dev.learn.flink.tablesql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @fileName: Use.java
 * @description: Use.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 4:04 下午
 */
public class UseTemporalTableFunction {
    public static void main(String[] args) {
        // 获取 stream 和 table 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

// 提供一个汇率历史记录表静态数据集
        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

// 用上面的数据集创建并注册一个示例表
// 在实际设置中，应使用自己的表替换它
        DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
        Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, $("r_currency"), $("r_rate"), $("r_proctime").proctime());

        tEnv.createTemporaryView("RatesHistory", ratesHistory);

        TemporalTableFunction temporalTableFunction = tEnv.from("RatesHistory")
                .createTemporalTableFunction($("r_proctime"), $("r_currency"));


        tEnv.createTemporaryFunction("Rates", temporalTableFunction);

    }
}
