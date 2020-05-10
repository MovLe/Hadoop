package HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HBASE_API {
    //获取配置文件
    private static Configuration conf = null;
    /**
     * 初始化
     * */
    static {
        //使用hbaseconfiguration获取conf
        conf = HBaseConfiguration.create();
        //注意使用主机名之前，确保win的hosts配置文件中配置Linux对应的ip和主机名的映射
        conf.set("hbase.zookeeper.quorum","192.168.31.132");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase");
    }
    /**
     * 判断表是否存在
     * */
    public static boolean is_table_exists(String table_Name) throws IOException {
        //1.创建hbase的客户端
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //2.判断表是否存在meta
        return admin.tableExists(Bytes.toBytes(table_Name));

    }

    public static void main(String[] args) throws IOException {
        //System.out.println(is_table_exists("aa"));;
        //create_table("idea1","cf1","cf2","cf3");
//        for (int i = 0;i<100;i++){
//            put_table("idea1",String.valueOf(i),"cf1","name","plus"+i);
//        }
        scan_table("idea1");
    }
    /**
     * 创建表:表名，列簇可以是多个
     * */
    public static void create_table(String table_name,String... columnFamily) throws IOException {
        //1.创建hbase的客户端
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        if(is_table_exists(table_name)){
            System.out.println(table_name+"已存在");
            return;
        }else {
            //创建表,创建一个表描述对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(table_name));
            //创建列簇
            for (String cf : columnFamily){
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(hTableDescriptor);
            System.out.println(table_name+"创建成功");
        }
    }
    /**
     * 插入数据:表名，rowkey，列簇，列，value
     * */
    public static void put_table(String name,String row,
                                 String columnfamily,String column,
                                 String value) throws IOException {
        //创建Htable
        HTable hTable = new HTable(conf, name);
        //创建put对象
        Put put = new Put(Bytes.toBytes(row));
        //添加列簇、列、数据
        put.add(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();

    }
    /**
     * 扫描数据
     * */
    public static void scan_table(String table_Name) throws IOException {
        HTable hTable = new HTable(conf, table_Name);

        //创建一个scan扫描region
        Scan scan = new Scan();
        //用htable创建resultScanner对象
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result: scanner){
            Cell[] cells = result.rawCells();
            for (Cell cell:cells){
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))+"\t"+
                        Bytes.toString(CellUtil.cloneFamily(cell))+","+
                        Bytes.toString(CellUtil.cloneQualifier(cell))+","+
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

    }

    public static void deleteMultiRow(String tableName,String... rows) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        List<Delete> deleteList = new ArrayList<Delete>();

        for(String row:rows){
            Delete delete = new Delete(Bytes.toBytes(row));

            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }
}