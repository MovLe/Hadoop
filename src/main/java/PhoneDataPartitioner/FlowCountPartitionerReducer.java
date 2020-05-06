package PhoneDataPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import phoneData.FlowBean;

import java.io.IOException;

/**
 * @ClassName FlowCountReducer
 * @MethodDesc: TODO FlowCountReducer功能介绍
 * @Author Movle
 * @Date 5/6/20 9:35 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FlowCountPartitionerReducer extends Reducer <Text,FlowPartitionerBean,Text,FlowPartitionerBean>{

    @Override
    protected void reduce(Text key, Iterable<FlowPartitionerBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow=0;
        long sum_downFlow=0;

        for(FlowPartitionerBean flowBean:values){
            sum_upFlow +=flowBean.getUpFlow();
            sum_downFlow +=flowBean.getDownFlow();
        }
        FlowPartitionerBean resultBean = new FlowPartitionerBean(sum_upFlow,sum_downFlow);

        context.write(key,resultBean);
    }
}
