package dev.learn.flink.sqlgateway;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @fileName: FlinkHiveSever2EndpointJdbc.java
 * @description: FlinkHiveSever2EndpointJdbc.java类说明
 * @author: huangshimin
 * @date: 2023/2/8 15:59
 */
public class FlinkHiveSever2EndpointJdbc {
    public static void main(String[] args) throws Exception {
        try (
                // Please replace the JDBC URI with your actual host, port and database.
                Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default;" +
                        "auth=noSasl");
                Statement statement = connection.createStatement()) {
//            statement.execute("SHOW TABLES");
            statement.execute("insert into source values(2,'b')");

            statement.execute("select * from source");
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1) + "--" + resultSet.getString(2));
            }

        }
    }
}
