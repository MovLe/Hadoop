package HBaseMR.HDFSToHBase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName Txt2FruitRunner
 * @MethodDesc: TODO Txt2FruitRunner功能介绍
 * @Author Movle
 * @Date 5/10/20 10:19 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class Txt2FruitRunner extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //得到Configuration
        Configuration conf = this.getConf();

        //创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Txt2FruitRunner.class);
        Path inPath = new Path("hdfs://hadoop2:9000/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);

        //设置Mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr2", WriteFruitMRFromTxtReducer.class, job);

        //设置Reduce数量，最少1个
        job.setNumReduceTasks(1);


        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("Job running with error");
        }


        return isSuccess ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new Txt2FruitRunner(), args);
        System.exit(status);
    }

}
