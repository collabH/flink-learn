package dev.flink.hudi.service;

/**
 * @fileName: HudiCurdDemo.java
 * @description: hudi curd操作服务
 * @author: huangshimin
 * @date: 2021/11/18 4:59 下午
 */
public interface HudiOperatorService<ENV, OPERATOR, COLLECTOR> {
    /**
     * 操作符号
     *
     * @param env       执行环境
     * @param operator  操作符
     * @param collector 数据收集器
     */
    void operation(ENV env, OPERATOR operator, COLLECTOR collector);
}
