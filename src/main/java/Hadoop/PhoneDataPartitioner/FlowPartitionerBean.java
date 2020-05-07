package Hadoop.PhoneDataPartitioner;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName FlowBean
 * @MethodDesc: TODO FlowBean功能介绍
 * @Author Movle
 * @Date 5/6/20 9:35 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/

// 1 实现writable接口
@Getter
@Setter
public class FlowPartitionerBean implements Writable {

    //上传流量
    private long upFlow;
    //下载流量
    private long downFlow;
    //流量总和
    private long sunFlow;

    //必须要有，反序列化要调用空参构造器
    public FlowPartitionerBean(){

    }

    public FlowPartitionerBean(long upFlow, long downFlow){
        this.upFlow = upFlow;
        this.downFlow=downFlow;
        this.sunFlow=upFlow+downFlow;
    }

    public void  set(long upFlow,long downFlow){
        this.upFlow = upFlow;
        this.downFlow=downFlow;
        this.sunFlow=upFlow+downFlow;
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sunFlow);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();
        this.sunFlow=in.readLong();
    }

    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sunFlow;
    }
}

