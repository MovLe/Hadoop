package PhoneDataPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName ProvincePartitioner
 * @MethodDesc: TODO ProvincePartitioner功能介绍
 * @Author Movle
 * @Date 5/6/20 11:25 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class ProvincePartitioner extends Partitioner<Text,FlowPartitionerBean> {
    @Override
    public int getPartition(Text key, FlowPartitionerBean value, int i) {

        String perNum = key.toString().substring(0,3);

        int partition =4;

        if("136".equals(perNum)){
            partition=0;
        }else if("137".equals(perNum)){
            partition=1;
        }else if("138".equals(perNum)){
            partition=2;
        }else {
            partition=3;
        }
        return partition;
    }
}
