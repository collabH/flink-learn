package dev.learn.flink.sql.grammar;

/**
 * @fileName: JoinFeature.java
 * @description: JoinFeature.java类说明
 * @author: huangshimin
 * @date: 2021/9/1 10:22 下午
 */
public class JoinFeature {
    public static void main(String[] args) {
        // 支持任意时间窗口的数据变化，但是会存在状态持续增大的问题，但是数据计算最实时最准确
        String regularJoin = "select * from order inner join product on order.id=product.id";

        // OUTER
        String outerLeftJoin = "SELECT * FROM Orders LEFT JOIN Product ON Orders.product_id = Product.id";
        String outerRightJoin = "SELECT * FROM Orders RIGHT JOIN Product ON Orders.product_id = Product.id";
        String fullOuterJoin = "SELECT * FROM Orders FULL OUTER JOIN Product ON Orders.product_id = Product.id";

        // 指定一段时间内的数据进行join,join近4个小时下单的数据
        String intervalJoin = "SELECT *\n" +
                "FROM Orders o, Shipments s\n" +
                "WHERE o.id = s.order_id\n" +
                "AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR AND s.ship_time";


        /**
         * 时态表join，提供数据版本
         * // system_time相当于processing time
         * SELECT [column_list]
         * FROM table1 [AS <alias1>]
         * [LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
         * ON table1.column-name1 = table2.column-name1
         *
         *
         */

          /*  -- Create a table of orders. This is a standard
            -- append-only dynamic table.
            CREATE TABLE orders (
                    order_id    STRING,
                    price       DECIMAL(32,2),
                    currency    STRING,
                    order_time  TIMESTAMP(3),
                    WATERMARK FOR order_time AS order_time
            ) WITH (*//* ... *//*);

            -- Define a versioned table of currency rates.
            -- This could be from a change-data-capture
                    -- such as Debezium, a compacted Kafka topic, or any other
            -- way of defining a versioned table.
                    CREATE TABLE currency_rates (
                    currency STRING,
                    conversion_rate DECIMAL(32, 2),
                    update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL
            WATERMARK FOR update_time AS update_time
    ) WITH (
                    'connector' = 'upsert-kafka',
                    *//* ... *//*
            );

            SELECT
                    order_id,
                    price,
                    currency,
                    conversion_rate,
                    order_time,
            FROM orders
            LEFT JOIN currency_rates FOR SYSTEM TIME AS OF orders.order_time
            ON orders.currency = currency_rates.currency

            order_id price currency conversion_rate  order_time
                    ====== ==== ======  ============  ========
            o_001    11.11  EUR        1.14                    12:00:00
            o_002    12.51  EUR        1.10                    12:0600*/

        /**
         * lookup join
         * 查找联接通常用于使用从外部系统查询的数据来丰富表。 连接要求一个表具有处理时间属性，另一个表由查找源连接器支持。
         * 通过connectorLookUpFunction实现
         */
       /* -- Customers is backed by the JDBC connector and can be used for lookup joins
        CREATE TEMPORARY TABLE Customers (
                id INT,
                name STRING,
                country STRING,
                zip STRING
        ) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:mysql://mysqlhost:3306/customerdb',
                'table-name' = 'customers'
        );

        -- enrich each order with customer information
        SELECT o.order_id, o.total, c.country, c.zip
        FROM Orders AS o
        JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
        ON o.customer_id = c.id;*/

        // 数组数据处理 array表达式
        String arraySql = "select order_id,tag from order cross join unnest(tags) as t(tag)";

        // todo table function
        String laterTableSql = "select id,id1 from a lateral table(table_func(id))t(id2)";
        //如果表函数调用返回空结果，则保留相应的外层行，并使用空值填充结果。目前，对侧表的左外连接需要在ON子句中使用TRUE。
//        SELECT order_id, res
//        FROM Orders
//        LEFT OUTER JOIN LATERAL TABLE(table_func(order_id)) t(res)
//        ON TRUE
    }
}
