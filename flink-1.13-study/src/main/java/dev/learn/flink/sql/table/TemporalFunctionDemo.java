package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @fileName: TemporalFunctionDemo.java
 * @description: 时态表函数
 * @author: huangshimin
 * @date: 2023/1/10 8:31 PM
 */
public class TemporalFunctionDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv);
        TemporalTableFunction rates = tEnv
                .from("currency_rates")
                .createTemporalTableFunction($("update_time"), $("currency"));
        tEnv.createTemporaryFunction("rates", rates);

        tEnv.sqlQuery("SELECT\n" +
                "  SUM(amount * rate) AS amount\n" +
                "FROM\n" +
                "  orders,\n" +
                "  LATERAL TABLE (rates(order_time))\n" +
                "WHERE\n" +
                "  rates.currency = orders.currency");
    }
}
