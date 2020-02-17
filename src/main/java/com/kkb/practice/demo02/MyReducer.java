package com.kkb.practice.demo02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text,FlowBean,Text,Text> {


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        for (FlowBean value : values) {
            int upFlow = 0;
            int downFlow = 0;
            int upCountFlow = 0;
            int downCountFlow = 0;

            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();

            context.write(key, new Text(upFlow + "\t" + downFlow +"\t" + upCountFlow +"\t" +downCountFlow+"\t" ));
        }
    }
}
