package Hadoop.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName WordCountReducer
 * @MethodDesc: TODO WordCountReducer功能介绍
 * @Author Movle
 * @Date 5/6/20 9:59 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //设置一个计数器
        int count=0;

        //利用循环叠加计数器
        for(IntWritable value:values){
            count += value.get();
        }
        //输出

        context.write(key,new IntWritable(count));
    }
}
