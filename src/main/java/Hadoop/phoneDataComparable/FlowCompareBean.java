package Hadoop.phoneDataComparable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName FlowBean
 * @MethodDesc: TODO FlowBean功能介绍
 * @Author Movle
 * @Date 5/6/20 10:28 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class FlowCompareBean implements WritableComparable<FlowCompareBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;


    public void set(long upFlow,long downFlow){
        this.upFlow=upFlow;
        this.downFlow=downFlow;
        this.sumFlow=upFlow+downFlow;
    }
    @Override
    public int compareTo(FlowCompareBean o) {
        return this.sumFlow > o.getSumFlow() ? -1:1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.upFlow=dataInput.readLong();
        this.downFlow=dataInput.readLong();
        this.sumFlow=dataInput.readLong();
    }

    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sumFlow;
    }
}
