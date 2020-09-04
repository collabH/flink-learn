package dev.learn.flink.tablesql.table;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: UseSetOperator.java
 * @description: UseSetOperator.java类说明
 * @author: by echo huang
 * @date: 2020/9/4 5:47 下午
 */
public class UseSetOperator {
    public static void main(String[] args) {
        TableEnvironment batchEnv = StreamEnvironment.getBatchEnv();

        Table table1 = batchEnv.fromValues(ROW(FIELD("id", INT().notNull()), FIELD("name", STRING().nullable())),
                row(1, "hsm"), row(1, "hsm"), row(2, "fdsf"), row(2, "fdsf"));
        Table table2 = batchEnv.fromValues(ROW(FIELD("id", INT().notNull()), FIELD("name", STRING().nullable())),
                row(3, "lis"), row(2, "fdsf"), row(2, "fdsf"));

        // union 去重，效率偏差
        table1.union(table2)
                .execute()
                .print();

        // union all 不去重
        table1.unionAll(table2)
                .execute()
                .print();

        // intersect 求交集
        table1.intersect(table2)
                .execute()
                .print();

        // intersectAll 类似于SQL INTERSECT ALL子句。 IntersectAll返回两个表中都存在的记录。 如果一个记录在两个表中都存在一次以上，则返回的次数与在两个表中都存在的次数相同，即结果表可能有重复的记录。 两个表必须具有相同的字段类型。
        table1.intersectAll(table2)
                .execute()
                .print();

        // minus a表和b表的差集，如果a表差集记录重复会去重复
        table1.minus(table2)
                .execute()
                .print();

        table1.minusAll(table2)
                .execute()
                .print();

        // in
        table1.select($("id"))
                .where($("id").in(table2.select($("id"))))
                .execute()
                .print();


    }
}
