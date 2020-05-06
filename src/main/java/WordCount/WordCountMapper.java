package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName WordCountMapper
 * @MethodDesc: wordcount程序的mapper
 * @Author Movle
 * @Date 5/6/20 9:14 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行数据
        String line = value.toString();

        //拆分成单词
        String[] fields = line.split(" ");

        //遍历每个单词
        for(String field:fields){

            //输出
            context.write(new Text(field),new IntWritable(1));
        }

    }


}
