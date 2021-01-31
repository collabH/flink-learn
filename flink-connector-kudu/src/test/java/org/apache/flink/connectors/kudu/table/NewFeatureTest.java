package org.apache.flink.connectors.kudu.table;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @fileName: NewFeatureTest.java
 * @description: NewFeatureTest.java类说明
 * @author: by echo huang
 * @date: 2020/12/28 5:31 下午
 */
public class NewFeatureTest {

    private KuduCatalog catalog;
    private StreamTableEnvironment tableEnv;

    @Before
    @Ignore
    public void init() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        catalog = new KuduCatalog("cdh01:7051,cdh02:7051,cdh03:7051");
        tableEnv = KuduTableTestUtils.createTableEnvWithBlinkPlannerStreamingMode(env);
        tableEnv.registerCatalog("kudu", catalog);
//        tableEnv.useCatalog("kudu");
    }

    @Test
    @Ignore
    public void testRangePartition() throws TableNotExistException, ExecutionException, InterruptedException, TimeoutException {
        tableEnv.useCatalog("kudu");
//        catalog.dropTable(new ObjectPath("default_database", "test_Replice_kudu"), true);
//        tableEnv.executeSql("create table test_Replice_kudu(id bigint,created_at string,name string)with('kudu.range-partition-rule'='created_at#2020,2021:created_at#2021','kudu.primary-key-columns'='id,created_at','kudu.replicas'='3','kudu.hash-partition-nums'='3','kudu.hash-columns'='id')");

        StatementSet statementSet = tableEnv.createStatementSet();
//        statementSet.addInsertSql("insert into test_Replice_kudu values(1,'2020-05-01','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(2,'2021-05-02','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(3,'2020-06-01','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(4,'2021-07-01','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(5,'2020-05-03','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(6,'2021-05-03','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(7,'2020-05-01','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(8,'2021-05-02','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(9,'2020-06-01','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(10,'2021-07-01','hsm')");
//        statementSet.addInsertSql("insert into test_Replice_kudu values(11,'2020-05-03','hsm')");
        statementSet.addInsertSql("insert into test_Replice_kudu values(14,'2021-05-03','hsm')");
        JobExecutionResult jobExecutionResult = statementSet.execute().getJobClient()
                .get()
                .getJobExecutionResult(this.getClass().getClassLoader())
                .get(1, TimeUnit.MINUTES);
//        tableEnv.executeSql("insert into testRange values(1,'hsm')");
    }

    @Test
    @Ignore
    public void testScanRowSizeConfig() {
        tableEnv.useCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG);
        tableEnv.executeSql("create table test_Replice_kudu(id bigint,created_at string,name string,proctime as proctime())with(" +
                "'connector.type'='kudu','kudu.masters'='cdh01:7051,cdh02:7051,cdh03:7051','kudu.scan.row-size'='2','kudu.primary-key-columns'='id')");
        tableEnv.executeSql("select * from test_Replice_kudu").print();
    }

    @Test
    @Ignore
    public void testLookUpFunction() {
        tableEnv.useCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG);
        tableEnv.executeSql("create table test_Replice_kudu(id bigint,created_at string,name string)with(" +
                "'connector.type'='kudu','kudu.masters'='cdh01:7051,cdh02:7051,cdh03:7051','kudu.lookup.cache.max-rows'='300','kudu.lookup.cache.ttl'='300000','kudu.primary-key-columns'='id')");

        tableEnv.executeSql("create table kafka_source_employment_test_user(id bigint,\nusername STRING,\npassword STRING,\nbirthday STRING,primary key(id) NOT ENFORCED," +
                " proctime as proctime())\nwith('connector'='kafka',\n'topic'='common_test1.employment_test.user',\n'properties.bootstrap.servers'='cdh04:9092,cdh05:9092,cdh06:9092',\n'properties.group.id'='kafka_source_employment_test_groups',\n'scan.startup.mode'= 'earliest-offset',\n'format'='debezium-json')");

//        tableEnv.executeSql("CREATE TABLE print_table WITH ('connector' = 'print')\n" +
//                "LIKE kafka_source_employment_test_user (EXCLUDING ALL)");
//        tableEnv.executeSql("insert into print_table select id,username,password,birthday from kafka_source_employment_test_user").print();

        tableEnv.executeSql("CREATE TABLE print_table(\n" +
                " id bigint,\n" +
                " created_at STRING,\n" +
                " name STRING,\n" +
                " username STRING,\n" +
                " password STRING,\n" +
                " birthday STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        tableEnv.executeSql("insert into print_table (select kafka_source_employment_test_user.id as id,test_Replice_kudu.created_at,test_Replice_kudu.name,username,password,birthday from kafka_source_employment_test_user left join " +
                "test_Replice_kudu FOR SYSTEM_TIME AS OF kafka_source_employment_test_user.proctime on test_Replice_kudu.id =kafka_source_employment_test_user.id)").print();

    }

    @Test
    public void testCreateDDL() {
        tableEnv.executeSql("CREATE TABLE TestTable1 (\n" +
                "  first STRING,\n" +
                "  `second` STRING\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kudu',\n" +
                "  'kudu.masters' = 'cdh01:7051,cdh02:7051,cdh03:7051',\n" +
                "  'kudu.table' = 'TestTable1',\n" +
                "  'kudu.hash-columns' = 'first',\n" +
                "  'kudu.primary-key-columns' = 'first,second'\n" +
                ")");
        tableEnv.executeSql("select * from TestTable1").print();
    }
}
