package com.kkb.practice.demo04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Text, Text,Text, IntWritable> {

    private Text text;

    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable(1);
    }

    /**
     hello@zolen@  input datas today
     count@zolen@  hadoop  spark
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,intWritable);
    }
}
