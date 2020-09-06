package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.Expressions.localTime;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: JoinOperator.java
 * @description: JoinOperator.java类说明
 * @author: by echo huang
 * @date: 2020/9/6 10:19 上午
 */
public class JoinOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        TableEnvironment tableEnv = StreamEnvironment.getBatchEnv();

        Table table = tableEnv.fromValues(ROW(FIELD("id", INT().notNull()), FIELD("name", VARCHAR(3).notNull()),
                FIELD("time1", TIMESTAMP(3).notNull())),
                row(1, "hsm", localTime()), row(2, "zzl", localTime()));
        Table table1 = tableEnv.fromValues(ROW(FIELD("aid", INT().notNull()), FIELD("user_id", INT().notNull()), FIELD("ext", VARCHAR(32).notNull()),
                FIELD("time", TIMESTAMP(3).notNull())),
                row(1, 1, "dxd", localTime()), row(2, 1, "dsd", localTime()),
                row(3, 2, "dd", localTime()), row(4, 3, "dsd", localTime()));

        tableEnv.createTemporaryView("user1", table);
        tableEnv.createTemporaryView("user2", table1);


        // inner join
        tableEnv.sqlQuery("select user1.id,user1.name,user2.aid from user1 inner join user2 on id=user_id")
                .execute()
                .print();

        //left outer join on
        tableEnv.sqlQuery("select user1.id,user1.name,user2.aid,user2.ext from user1 left join user2 on user1.id=user2.user_id")
                .execute()
                .print();

        //right outer join on
        tableEnv.sqlQuery("select user1.id,user2.aid,user2.ext from user1 right join user2 on user1.id=user2.user_id")
                .execute()
                .print();

        // full join
        tableEnv.sqlQuery("select user1.id,user1.name,user2.aid,user2.ext from user1 full outer join user2 on user1.id=user2.user_id")
                .execute()
                .print();

        //interval
//        tableEnv.sqlQuery("SELECT *\n" +
//                "FROM Orders o, Shipments s\n" +
//                "WHERE o.id = s.orderId AND\n" +
//                "      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime");
    }
}
