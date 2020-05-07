package Hadoop.phoneData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName FlowCountReducer
 * @MethodDesc: TODO FlowCountReducer功能介绍
 * @Author Movle
 * @Date 5/6/20 9:35 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FlowCountReducer extends Reducer <Text,FlowBean,Text,FlowBean>{

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow=0;
        long sum_downFlow=0;

        for(FlowBean flowBean:values){
            sum_upFlow +=flowBean.getUpFlow();
            sum_downFlow +=flowBean.getDownFlow();
        }
        FlowBean resultBean = new FlowBean(sum_upFlow,sum_downFlow);

        context.write(key,resultBean);
    }
}
