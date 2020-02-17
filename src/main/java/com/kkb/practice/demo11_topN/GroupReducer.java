package com.kkb.practice.demo11_topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<OrderBean,DoubleWritable,OrderBean,DoubleWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        //需要对我们集合只输出两个值
        int i = 0;
        for (DoubleWritable value : values) {
            if(i<2){
                context.write(key,value);
                i ++;
            }else{
                break;
            }
        }
    }
}
