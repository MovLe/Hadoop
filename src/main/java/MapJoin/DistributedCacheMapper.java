package MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName DistributedCacheMapper
 * @MethodDesc: TODO DistributedCacheMapper功能介绍
 * @Author Movle
 * @Date 5/7/20 7:58 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class DistributedCacheMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Map<String,String> pdMap = new HashMap<>();

    /**
     * 初始化方法
     * 把pd.txt加载进来
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //把pd.txt文件加载进来，这里的路径是本地的路径，若是在mac里面运行那就是mac里面文件地址，若是在linux里运行则是linux里面的pd.txt地址
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/opt/TestFolder/pd.txt")),"UTF-8"));
        String line;

        while(StringUtils.isNotEmpty(line=reader.readLine())){
            String[] fields = line.split("\t");

            String pid = fields[0];
            String pname= fields[1];

            pdMap.put(pid,pname);
        }
        reader.close();

    }

    Text k = new Text();

    /**
     * order.txt的处理
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String orderId = fields[0];
        String pid = fields[1];
        String amount = fields[2];

        String pname = pdMap.get(pid);

        k.set(orderId+"\t"+pname+"\t"+amount);

        context.write(k,NullWritable.get());

    }
}
