package com.kkb.practice.demo05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text text;

    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable();
        text = new Text();
        intWritable.set(1);
    }

    /**
     * hello,hello
     * world,world
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");
        for (String s : split) {
            text.set(s);
            context.write(text,intWritable);
        }
    }
}
