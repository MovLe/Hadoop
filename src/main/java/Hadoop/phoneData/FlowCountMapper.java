package Hadoop.phoneData;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName FlowCountMapper
 * @MethodDesc: TODO FlowCountMapper功能介绍
 * @Author Movle
 * @Date 5/6/20 9:35 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/

/**
 * LongWritable, Text ===> Map输入    <偏移量，手机号>
 * Text, FlowBean  ======> Map的输出：<手机号、流量上传下载总和>
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phoneNum =fields[1];
        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);

        k.set(phoneNum);

        context.write(k,new FlowBean(upFlow,downFlow));

    }
}
