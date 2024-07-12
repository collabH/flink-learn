package dev.learn.flink.jdbcdriver;

import org.apache.flink.table.jdbc.FlinkDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * @fileName: FlinkDatasourceDriver.java
 * @description: FlinkDatasourceDriver.java类说明
 * @author: huangshimin
 * @date: 2024/7/12 10:58
 */
public class FlinkDatasourceDriver {
    public static void main(String[] args) throws Exception {
        DataSource dataSource = new FlinkDataSource("jdbc:flink://localhost:8083", new Properties());
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE T(\n" +
                "  a INT,\n" +
                "  b VARCHAR(10)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'number-of-rows' = '100000'\n"+
                ")");
        try (ResultSet rs = statement.executeQuery("SELECT * FROM T Limit 100")) {
            while (rs.next()) {
                System.out.println(rs.getInt(1) + ", " + rs.getString(2));
            }
        }
    }
}
