package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: InsertOperator.java
 * @description: InsertOperator.java类说明
 * @author: by echo huang
 * @date: 2020/9/6 2:04 下午
 */
public class InsertOperator {
    public static void main(String[] args) {
        TableEnvironment env = StreamEnvironment.getBatchEnv();

        env.createTemporaryView("User", env.fromValues(ROW(FIELD("id", INT()), FIELD("name", VARCHAR(32))),
                row(1, "hsm"), row(2, "name")));


        env.executeSql("drop table  if exists user1");

        env.executeSql("create table user1(id int,name string)");

        // insert single sql
        env.executeSql("insert into user1 select id,name from User");

        // insert mutli sql
        StatementSet statementSet = env.createStatementSet();
        statementSet.addInsertSql("insert into user1 select id,name from User");
        statementSet.addInsertSql("insert into user1 select id,name from User");
    }
}
