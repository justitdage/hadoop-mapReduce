package com.kkb.practice.demo02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    private FlowBean flowBean;
    private Text phone;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
        phone = new Text();
    }

    /**
     * 1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com	游戏娱乐	24	27	2481	24681	200
     * 1363157995052	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4	jd.com	京东购物	4	0	264	0	200
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        phone.set(split[0]);

        flowBean.setUpFlow(Integer.valueOf(split[6]));
        flowBean.setDownFlow(Integer.valueOf(split[7]));
        flowBean.setUpCountFlow(Integer.valueOf(split[8]));
        flowBean.setDownCountFlow(Integer.valueOf(split[9]));
        context.write(phone,flowBean);
    }
}
