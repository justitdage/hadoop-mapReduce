package com.kkb.practice.demo06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    //当前分片的信息
    private FileSplit fileSplit;

    //当前分片的数据
    private BytesWritable bytesWritable;

    private boolean flag = false;

    //文件系统
    private Configuration configuration;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) split;
        configuration = context.getConfiguration();
        bytesWritable = new BytesWritable();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!flag){
            //分片大小
            int length = (int) fileSplit.getLength();
            Path path = fileSplit.getPath();
            byte[] bytes = new byte[length];
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            IOUtils.readFully(inputStream,bytes,0,length);
            bytesWritable.set(bytes,0,length);
            flag = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return flag?1.0f:0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
