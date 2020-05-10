package HBaseMR.HBaseToHBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName FruitToFruitMRRunner
 * @MethodDesc: TODO FruitToFruitMRRunner功能介绍
 * @Author Movle
 * @Date 5/10/20 9:32 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FruitToFruitMRRunner extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //得到Configuration
        Configuration conf = this.getConf();
        //创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(FruitToFruitMRRunner.class);

        //配置Job,创建一个扫描器
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        //设置Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit",
                scan,
                ReadFruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
        );
//        TableMapReduceUtil.initTableMapperJob(
//                "fruit",  //读数据的表
//                scan,  //扫描器
//                ReadFruitMapper.class, //设置map类
//                ImmutableBytesWritable.class,  //设置输出的key类型
//                Put.class,  //设置Mapper输出value值类型
//                job  //配置的job
//        );
        //设置Reducer
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr",
                WriteFruitMRReducer.class,
                job);
//        TableMapReduceUtil.initTableReducerJob(
//                "fruit_mr",  //将数据戏写入的表
//                WriteFruitMRReducer.class,  //设置reduce类
//                job);

        //设置Reduce数量，最少1个
        job.setNumReduceTasks(1);


        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new FruitToFruitMRRunner(), args);
        System.exit(status);
    }

}
