package HBaseUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;
/**
 * @ClassName HBASEUTIL
 * @MethodDesc: TODO HBASEUTIL功能介绍
 * @Author Movle
 * @Date 5/10/20 8:04 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class HBASEUtil {

    /**
     * 创建命名
     * */
    public static void create_Namecpace(Configuration conf,String namecpace) throws IOException {
        //获取hbase的客户端
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //创建命名描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namecpace).
                addConfiguration("author","plus").build();

        //创建命名空间
        admin.createNamespace(namespaceDescriptor);
        System.out.println("初始化命名空间");
        close(admin, connection);
    }

    /**
     * 关闭资源
     * */
    private static void close(Admin admin, Connection connection) throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 创建HBase的表
     * @param conf
     * @param tableName
     * @param regions
     * @param columnFamily
     */
    public static void createTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //判断表
        if (isExistTable(conf, tableName)) {
            return;
        }
        //表描述器 HTableDescriptor

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamily) {
        //列描述器 ：HColumnDescriptor
            htd.addFamily(new HColumnDescriptor(cf));
        }
        //htd.addCoprocessor("hbase.CalleeWriteObserver");
        //创建表
        admin.createTable(htd,genSplitKeys(regions));
        System.out.println("已建表");
        //关闭对象
        close(admin,connection);
    }

    /**
     * 分区键
     * @param regions region个数
     * @return splitKeys
     */
    private static byte[][] genSplitKeys(int regions) {
        //存放分区键的数组
        String[] keys = new String[regions];
        //格式化分区键的形式  00 01 02
        DecimalFormat df = new DecimalFormat("000");
        for (int i = 0; i < regions; i++) {
            keys[i] = df.format(i*10) + "";
        }


        byte[][] splitKeys = new byte[regions][];
        //排序 保证你这个分区键是有序得
        TreeSet<byte[]> treeSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        //输出
        Iterator<byte[]> iterator = treeSet.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            byte[] next = iterator.next();
            splitKeys[index++]= next;
        }

        return splitKeys;
    }


    /**
     * 判断表是否存在
     * @param conf      配置 conf
     * @param tableName 表名
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();


        boolean result = admin.tableExists(TableName.valueOf(tableName));
        close(admin, connection);
        return result;
    }
}


