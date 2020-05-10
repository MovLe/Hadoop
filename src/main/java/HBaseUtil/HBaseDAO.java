package HBaseUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
/**
 * @ClassName HBaseDAO
 * @MethodDesc: TODO HBaseDAO功能介绍
 * @Author Movle
 * @Date 5/10/20 8:06 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class HBaseDAO {


    private static String namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
    private static String tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
    private static Integer regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "bigdata111");
        conf.set("zookeeper.znode.parent", "/hbase");


        if (!HBASEUtil.isExistTable(conf, tableName)) {
            HBASEUtil.create_Namecpace(conf, namespace);
            HBASEUtil.createTable(conf, tableName, regions, "f1", "f2");
        }
    }
}

