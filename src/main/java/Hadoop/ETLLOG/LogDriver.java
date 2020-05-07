package Hadoop.ETLLOG;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName LogDriver
 * @MethodDesc: TODO LogDriver功能介绍
 * @Author Movle
 * @Date 5/6/20 5:24 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //获取配置信息
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        //加载反射类
        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LoggerMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //输入数据和输出数据的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //job提交
        job.waitForCompletion(true);

    }

}
