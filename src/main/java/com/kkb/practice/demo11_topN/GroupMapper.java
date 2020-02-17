package com.kkb.practice.demo11_topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable,Text,OrderBean,DoubleWritable> {

    /**
     * Order_0000005    Pdt_01	222.8
     Order_0000005	Pdt_05	25.8
     Order_0000002	Pdt_03	322.8
     Order_0000002	Pdt_04	522.4
     Order_0000002	Pdt_05	822.4
     Order_0000003	Pdt_01	222.8
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        OrderBean orderBean = new OrderBean();
        orderBean.setOrderId(split[0]);
        orderBean.setPrice(Double.valueOf(split[2]));

        //输出orderBean  price
        context.write(orderBean,new DoubleWritable(Double.valueOf(split[2])));



    }
}
