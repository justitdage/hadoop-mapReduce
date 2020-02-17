package com.kkb.practice.demo10_top1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupPartition extends Partitioner<OrderBean,NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        //(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;

        //注意这里：使用orderId作为分区的条件，来进行判断，保证相同的orderId进入到同一个reduceTask里面去
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
