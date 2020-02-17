package com.kkb.practice.demo11_topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupPartition extends Partitioner<OrderBean,DoubleWritable> {
    @Override
    public int getPartition(OrderBean orderBean, DoubleWritable doubleWritable, int numReduceTasks) {
        //(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;

        //注意这里：使用orderId作为分区的条件，来进行判断，保证相同的orderId进入到同一个reduceTask里面去
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
