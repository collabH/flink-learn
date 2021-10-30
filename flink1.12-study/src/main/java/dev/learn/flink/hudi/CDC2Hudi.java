package dev.learn.flink.hudi;

/**
 * @fileName: CDC2Hudi.java
 * @description: flink cdc sink hudi
 * @author: huangshimin
 * @date: 2021/10/30 8:07 下午
 */
public class CDC2Hudi {
    private static String getCDCMysqlSource(){
        return "create table cdc_source()with()";
    }
}
