package dev.learn.flink.jdbcdriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @fileName: FlinkSQLJDBCDriver.java
 * @description: FlinkSQLJDBCDriver.java类说明
 * @author: huangshimin
 * @date: 2024/7/12 10:41
 */
public class FlinkSQLJDBCDriver {
    public static void main(String[] args) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:flink://localhost:8083")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE T(\n" +
                        "  a INT,\n" +
                        "  b VARCHAR(10)\n" +
                        ") WITH (\n" +
                        "  'connector' = 'datagen',\n" +
                        "  'number-of-rows' = '100000'\n"+
                        ")");
                try (ResultSet rs = statement.executeQuery("SELECT * FROM T")) {
                    while (rs.next()) {
                        System.out.println(rs.getInt(1) + ", " + rs.getString(2));
                    }
                }
            }
        }
    }
}
