package dev.learn.debezium;

import java.util.Properties;

/**
 * @fileName: DebeziumMysqlDemo.java
 * @description: DebeziumMysqlDemo.java类说明
 * @author: by echo huang
 * @date: 2020/11/12 9:36 上午
 */
public class DebeziumMysqlDemo {
    public static void main(String[] args) {
        Properties props=new Properties();
        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.KafkaOffsetBackingStore");
        props.setProperty("offset.storage.topic", "test");
        props.setProperty("offset.storage.partitions", "3");
        props.setProperty("offset.storage.replication.factor", "2");
        props.setProperty("offset.commit.policy", "io.debezium.embedded.spi.OffsetCommitPolicy.AlwaysCommitOffsetPolicy");
        props.setProperty("offset.flush.interval.ms", "60000");
        /* begin connector properties */
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "mysqluser");
        props.setProperty("database.password", "mysqlpw");
        props.setProperty("database.server.id", "85744");
        props.setProperty("database.server.name", "my-app-connector");
        props.setProperty("database.history",
                "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename",
                "/path/to/storage/dbhistory.dat");
    }
}
