package Hadoop.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName WordCountDriver
 * @MethodDesc: TODO WordCountDriver功能介绍
 * @Author Movle
 * @Date 5/6/20 10:07 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //args = new String[]{"/Users/macbook/TestInfo/a.txt","/Users/macbook/TestInfo/WC"};

        //1.获取配置信息，或job对象实例
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //map输出的k,v,reduce的输入k,v
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce输出的k,v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置分区，也要设置运算分区的reduce 的task数,因为分区数是2，所以这里设置为2
        job.setPartitionerClass(WordCountPartitioner.class);
        job.setNumReduceTasks(2);

        //指定job的输入原始文件所在目录，以及输出目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交job，waitForCompletion包含job.submit
        job.waitForCompletion(true);


    }
}
