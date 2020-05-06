package HDFSAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * @ClassName HdfsClientDemo1
 * @MethodDesc: TODO HdfsClientDemo1功能介绍
 * @Author Movle
 * @Date 5/5/20 4:28 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class HdfsAPI {

    public static void main(String[] args) throws Exception{

        //putFileToHdfs();

        //getFileFromHdfs();

        //mkdirHdfs();

        //deleteHdfs();

        //renameHdfsFile();

        //jugde();

        //putFileToHdfsIO();

        getFileFormHdfsIO();
    }

    public static void putFileToHdfs() throws Exception {

        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.31.132:9000"),configuration,"root");

        fileSystem.copyFromLocalFile(new Path("/Users/macbook/TestInfo/a.txt"),new Path("hdfs://192.168.31.132:9000/aa.txt"));

        fileSystem.close();

        System.out.println("上传成功！");

    }

    public static void getFileFromHdfs() throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.31.132:9000"),configuration,"root");

        fileSystem.copyToLocalFile(false,new Path("hdfs://192.168.31.132:9000/aa.txt"),new Path("/Users/macbook/TestInfo/downloadFileFromHdfs.txt"));

        fileSystem.close();

        System.out.println("下载成功");
    }

    public static void mkdirHdfs() throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.31.132:9000"),configuration,"root");

        fileSystem.mkdirs(new Path("hdfs://192.168.31.132:9000/HdfsApiMkdir"));

        fileSystem.close();
        System.out.println("创建HDFS目录成功");
    }
    public static void deleteHdfs() throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.31.132:9000"),configuration,"root");

        fileSystem.delete(new Path("hdfs://192.168.31.132:9000/HdfsApiMkdir"),false);

        fileSystem.close();
        System.out.println("删除hdfs文件夹成功！");

    }

    public static void renameHdfsFile() throws Exception{
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.31.132:9000"),configuration,"root");

        fileSystem.rename(new Path("hdfs://192.168.31.132:9000/a.txt"),new Path("hdfs://192.168.31.132:9000/b.txt"));

        fileSystem.close();

        System.out.println("文件名更改成功！");

    }

    public static void jugde() throws Exception {

        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.31.132:9000"),configuration,"root");


        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/Movle"));

        for(FileStatus status:fileStatuses){

            if(status.isFile()){
                System.out.println("文件："+status.getPath().getName());
            }else {
                System.out.println("目录："+status.getPath().getName());

            }
        }
    }


    public static void putFileToHdfsIO() throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.31.132:9000"),configuration,"root");

        FileInputStream fis = new FileInputStream(new File("/Users/macbook/TestInfo/a.txt"));

        Path writePath = new Path("hdfs://192.168.31.132:9000/Andy/IOa.txt");

        FSDataOutputStream fos = fileSystem.create(writePath);


        try {
            IOUtils.copyBytes(fis,fos,4*1024,false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(fos);
            IOUtils.closeStream(fis);

            System.out.println("IO流上传成功！");
        }

    }

    public static void getFileFormHdfsIO() throws Exception {

        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.31.132:9000"),configuration,"root");


        FileOutputStream fos = new FileOutputStream(new File("/Users/macbook/TestInfo/downloadFileFromHdfsIO.txt"));

        Path readPath = new Path("hdfs://192.168.31.132:9000/aa.txt");

        FSDataInputStream fis = fileSystem.open(readPath);

        try {
            IOUtils.copyBytes(fis,fos,4*1024,false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
            System.out.println("下载成功！");


        }


    }

}
