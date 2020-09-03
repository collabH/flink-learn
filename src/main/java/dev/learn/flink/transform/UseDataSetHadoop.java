package dev.learn.flink.transform;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @fileName: UseDataSetHadoop.java
 * @description: use hadoop dataset
 * @author: by echo huang
 * @date: 2020/9/3 10:21 上午
 */
public class UseDataSetHadoop {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // hadoop input format
        DataSource<Tuple2<LongWritable, Text>> hadoopDataSource = env.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(), LongWritable.class, Text.class, "hdfs://hadoop:8020/kylin/test.txt"));


//        hadoopDataSource.print();

        // hadoop output format
        Job job = Job.getInstance();
        HadoopOutputFormat<Text, IntWritable> hadoopOF =
                // create the Flink wrapper.
                new HadoopOutputFormat<Text, IntWritable>(
                        // set the Hadoop OutputFormat and specify the job.
                        new TextOutputFormat<Text, IntWritable>(), job
                );
        hadoopOF.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        TextOutputFormat.setOutputPath(job, new Path("hdfs://hadoop:8020/kylin/test1.txt"));

        hadoopDataSource.flatMap(new HadoopMapFunction<>(new MapTask()))
                .output(hadoopOF);
    }
}

class MapTask implements Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        long offset = key.get();
        int count = (int) (offset + 10);
        outputCollector.collect(value, new IntWritable(count));
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
