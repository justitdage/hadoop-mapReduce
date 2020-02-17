package com.kkb.practice.demo07;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //手机号
        String phone = text.toString();
        if(StringUtils.isNotBlank(phone)){
            if(phone.startsWith("135")){
                return 0;
            }else if(phone.startsWith("136")){
                return 1;
            }else if(phone.startsWith("137")){
                return 2;
            }else if(phone.startsWith("138")){
                return 3;
            }else if(phone.startsWith("139")){
                return 4;
            }else {
                return 5;
            }
        }
        return 5;
    }
}
