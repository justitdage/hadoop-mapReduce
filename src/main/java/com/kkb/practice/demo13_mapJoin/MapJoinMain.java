package com.kkb.practice.demo13_mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class MapJoinMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        URI uri = new URI("hdfs://node01:8020/cache/pdts.txt");
        Configuration conf = super.getConf();
        //添加缓存文件
        DistributedCache.addCacheFile(uri,conf);
        //获取job对象
        Job job = Job.getInstance(conf, "mapJoin");
        //读取文件，解析成为key，value对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\开课吧课程资料\\Hadoop&ZooKeeper课件\\最新版本课件\\hadoop与zookeeper课件资料\\3、第三天\\11、join操作\\数据\\map端join\\map_join_iput"));
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //没有reducer逻辑，不用设置了
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\开课吧课程资料\\Hadoop&ZooKeeper课件\\最新版本课件\\hadoop与zookeeper课件资料\\3、第三天\\11、join操作\\数据\\map端join\\output_result"));



        boolean b = job.waitForCompletion(true);


        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new MapJoinMain(), args);
        System.exit(run);
    }

}
