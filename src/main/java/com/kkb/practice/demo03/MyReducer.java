package com.kkb.practice.demo03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable count;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        count = new IntWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable value : values) {
            result += value.get();
        }
        count.set(result);
        context.write(key,count);
    }
}
