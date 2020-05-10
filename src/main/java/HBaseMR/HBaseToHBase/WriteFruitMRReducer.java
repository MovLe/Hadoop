package HBaseMR.HBaseToHBase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @ClassName WriteFruitMRReducer
 * @MethodDesc: TODO WriteFruitMRReducer功能介绍
 * @Author Movle
 * @Date 5/10/20 9:29 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WriteFruitMRReducer extends TableReducer<ImmutableBytesWritable, Put,NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
            throws IOException, InterruptedException {
        //读出来的每一行数据写入到fruit_mr表中
        for(Put put: values){
            context.write(NullWritable.get(), put);
        }
    }

}
