package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName WordCountPartitioner
 * @MethodDesc: TODO WordCountPartitioner功能介绍
 * @Author Movle
 * @Date 5/6/20 11:18 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int i) {
        // 1.获取单词key
        String word = key.toString();
        // 2.获取单词长度
        int length= key.getLength();

        // 3.按照规则自定义分区
        if(length%2==0){
            return 1;
        }else {
            return 0;
        }

    }
}
