package Hadoop.MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @ClassName DistributedCacheDriver
 * @MethodDesc: TODO DistributedCacheDriver功能介绍
 * @Author Movle
 * @Date 5/7/20 8:19 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class DistributedCacheDriver {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(DistributedCacheDriver.class);

        job.setMapperClass(DistributedCacheMapper.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //这里的路径是hdfs上的路径
        job.setCacheFiles(new URI[]{new URI("/pd.txt")});

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);

    }
}
