package com.kkb.practice.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        //获取job对象
        Job job = Job.getInstance(conf, "wordCount");
        //实际工作当中，程序运行完成之后一般都是打包到集群上面去运行，打成一个jar包
        //如果要打包到集群上面去运行，必须添加以下设置
        job.setJarByClass(WordCount.class);

        //设置inputFormat
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job,new Path("file:///D:\\work\\code\\ziliao\\1、wordCount_input\\数据"));

        //设置map  输入输出
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置outPutFormat
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///D:\\work\\code\\ziliao\\1、wordCount_input\\数据\\out"));

        boolean result = job.waitForCompletion(true);

        return result ? 0:1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf,new WordCount(),args);
        System.exit(run);
    }
}
