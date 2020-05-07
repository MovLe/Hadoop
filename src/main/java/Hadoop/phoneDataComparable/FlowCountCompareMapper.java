package Hadoop.phoneDataComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName FlowCountCompareMapper
 * @MethodDesc: TODO FlowCountCompareMapper功能介绍
 * @Author Movle
 * @Date 5/6/20 10:33 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FlowCountCompareMapper extends Mapper <LongWritable, Text, FlowCompareBean,Text> {

    FlowCompareBean k =new FlowCompareBean();
    Text v =new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phoneNum = fields[0];

        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);

        k.set(upFlow,downFlow);

        v.set(phoneNum);

        context.write(k,v);

    }
}
