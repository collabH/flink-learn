package org.apache.flink.connectors.kudu.table;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.Before;
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
    public void init() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        catalog = new KuduCatalog("cdh01:7051,cdh02:7051,cdh03:7051");
        tableEnv = KuduTableTestUtils.createTableEnvWithBlinkPlannerStreamingMode(env);
        tableEnv.registerCatalog("kudu", catalog);
//        tableEnv.useCatalog("kudu");
    }

    @Test
    public void testRangePartition() throws TableNotExistException, ExecutionException, InterruptedException, TimeoutException {
        tableEnv.useCatalog("kudu");
        catalog.dropTable(new ObjectPath("default_database", "test_Replice_kudu"), true);
        tableEnv.executeSql("create table test_Replice_kudu(id bigint,created_at string,name string)with('kudu.range-partition-rule'='created_at#2020,2021:created_at#2021','kudu.primary-key-columns'='id,created_at','kudu.replicas'='3','kudu.hash-partition-nums'='3','kudu.hash-columns'='id')");

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("insert into test_Replice_kudu values(1,'2020-05-01','hsm')");
        statementSet.addInsertSql("insert into test_Replice_kudu values(2,'2021-05-02','hsm')");
        statementSet.addInsertSql("insert into test_Replice_kudu values(3,'2020-06-01','hsm')");
        statementSet.addInsertSql("insert into test_Replice_kudu values(4,'2021-07-01','hsm')");
        statementSet.addInsertSql("insert into test_Replice_kudu values(5,'2020-05-03','hsm')");
        statementSet.addInsertSql("insert into test_Replice_kudu values(6,'2021-05-03','hsm')");
        JobExecutionResult jobExecutionResult = statementSet.execute().getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);
//        tableEnv.executeSql("insert into testRange values(1,'hsm')");
    }

    @Test
    public void testMapping() {
        tableEnv.useCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG);
        tableEnv.executeSql("create table test_Replice_kudu(id bigint,created_at string,name string,ts AS TO_TIMESTAMP(created_at),WATERMARK FOR ts AS ts - INTERVAL '60' SECOND\n)with(" +
                "'connector.type'='kudu','kudu.masters'='cdh01:7051,cdh02:7051,cdh03:7051','kudu.primary-key-columns'='id')");
        tableEnv.executeSql("select id,TUMBLE_START(ts, INTERVAL '10' MINUTE) as start_ts,TUMBLE_END(ts, INTERVAL '10' MINUTE) as end_ts from test_Replice_kudu group by TUMBLE(ts, INTERVAL '10' MINUTE) ,id\n")
                .print();
    }
}
