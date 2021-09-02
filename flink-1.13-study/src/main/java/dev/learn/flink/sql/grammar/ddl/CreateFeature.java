package dev.learn.flink.sql.grammar.ddl;

/**
 * @fileName: CreateFeature.java
 * @description: ddl create
 * @author: huangshimin
 * @date: 2021/9/2 11:29 上午
 */
public class CreateFeature {
    public static void main(String[] args) {
        // 定义watermark
        /**
         * WATERMARK FOR rowtime_column_name AS watermark_strategy_expression 。
         * 严格递增时间戳： WATERMARK FOR rowtime_column AS rowtime_column。
         * 递增时间戳： WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND。
         * 有界乱序时间戳： WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit
         */

        // PRIMARY KEY
        /**
         * 主键列定义，主键列定义都是非nullable
         * SQL 标准主键限制可以有两种模式：ENFORCED 或者 NOT ENFORCED。 它申明了是否输入/出数据会做合法性检查（是否唯一）。Flink 不存储数据因此只支持 NOT ENFORCED 模式，即不做检查，用户需要自己保证唯一性。
         */

        // LIKE
        /**
         * 基于Orders表的列定义去创建支持watermarker的表
         * CREATE TABLE Orders (
         *     `user` BIGINT,
         *     product STRING,
         *     order_time TIMESTAMP(3)
         * ) WITH (
         *     'connector' = 'kafka',
         *     'scan.startup.mode' = 'earliest-offset'
         * );
         *
         * CREATE TABLE Orders_with_watermark (
         *     -- 添加 watermark 定义
         *     WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
         * ) WITH (
         *     -- 改写 startup-mode 属性
         *     'scan.startup.mode' = 'latest-offset'
         * )
         * LIKE Orders;
         *
         * 表合并策略也可以控制，默认为INCLUDING ALL OVERWRITING OPTIONS
         * 可以控制合并的表属性如下：
         *
         * CONSTRAINTS - 主键和唯一键约束
         * GENERATED - 计算列
         * OPTIONS - 连接器信息、格式化方式等配置项
         * PARTITIONS - 表分区信息
         * WATERMARKS - watermark 定义
         * 并且有三种不同的表属性合并策略：
         *
         * INCLUDING - 新表包含源表（source table）所有的表属性，如果和源表的表属性重复则会直接失败，例如新表和源表存在相同 key 的属性。
         * EXCLUDING - 新表不包含源表指定的任何表属性。
         * OVERWRITING - 新表包含源表的表属性，但如果出现重复项，则会用新表的表属性覆盖源表中的重复表属性，例如，两个表中都存在相同 key 的属性，则会使用当前语句中定义的 key 的属性值。
         * -- 对应存储在 kafka 的源表
         * CREATE TABLE Orders_in_kafka (
         *     -- 添加 watermark 定义
         *     WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
         * ) WITH (
         *     'connector' = 'kafka',
         *     ...
         * )
         * LIKE Orders_in_file (
         *     -- 排除需要生成 watermark 的计算列之外的所有内容。
         *     -- 去除不适用于 kafka 的所有分区和文件系统的相关属性。
         *     EXCLUDING ALL
         *     INCLUDING GENERATED
         * );
         */
    }

}
