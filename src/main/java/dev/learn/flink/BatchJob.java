/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.learn.flink;

import com.google.common.collect.Lists;
import dev.learn.flink.function.SumReduceFunction;
import dev.learn.flink.function.WordCountMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromCollection(Lists.newArrayList(
                "spark",
                "flink",
                "hadoop",
                "hadoop",
                "flume",
                "flume",
                "kafka",
                "kafka",
                "spark-streaming",
                "spark-streaming",
                "sqoop",
                "azkaban",
                "azkaban",
                "yarn",
                "hdfs",
                "mapreduce",
                "spark-sql",
                "spark-core",
                "hive",
                "hive",
                "hbase",
                "zookeeper",
                "spark",
                "spark-sql",
                "spark-sql",
                "flink",
                "flink"
        ));
        dataSource.map(new WordCountMapFunction())
                .groupBy(0)
                .reduce(new SumReduceFunction())
                .print();

//        env.execute("Flink Batch Java API Skeleton");
    }
}
