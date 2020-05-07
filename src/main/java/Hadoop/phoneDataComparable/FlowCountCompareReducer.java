package Hadoop.phoneDataComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName FlowCountCompareReducer
 * @MethodDesc: TODO FlowCountCompareReducer功能介绍
 * @Author Movle
 * @Date 5/6/20 10:38 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FlowCountCompareReducer extends Reducer<FlowCompareBean, Text,Text, FlowCompareBean> {

    @Override
    protected void reduce(FlowCompareBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text text:values){
            context.write(text,key);
        }
    }
}
