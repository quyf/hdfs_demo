package com.quyf.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2018/4/17.
 */
public class HdfsTest {

    public static FileSystem getFs() throws Exception{
        //方式一：添加xml文件，这样就可以直接访问ha hdfs了
//        Configuration conf = new Configuration();
//        conf.addResource("classpath:/core-site.xml");
//        conf.addResource("classpath:/hdfs-site.xml");
//        conf.addResource("classpath:/mapred-site.xml");
        //FileSystem fs = FileSystem.get(new URI("hdfs://quyf-nn-cluster"), conf, "bigdata");

        //方式二，沒有配置文件(可把resource下xml文件删除)，如果hdfs做了ha的話，那只能访问其中namenode是active角色的namenode
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.245.133:8020"), new Configuration(), "bigdata");
        return fs;
    }

    @Test
    public void testList() throws Exception {
        FileSystem fs = getFs();
        FileStatus[] fileStatus = fs.listStatus(new Path("/"));
        for (FileStatus status:fileStatus){
            System.out.println( status.toString());
        }

        Path[] paths = FileUtil.stat2Paths(fileStatus);
        for(Path p:paths){
            System.out.println(p.toString());
        }
        fs.close();
    }

    /**
     * 1、利用封装好的api
     * @throws Exception
     */
    @Test
    public void testCopyFromLocal()throws  Exception{
        FileSystem fs = getFs();
        String srcFile = "I:/yuewen-util.jar";
        String destFile = "/test/"; //上传到/test/目录下
        Path destPath =  new Path(destFile);
        if( !fs.exists(destPath)){
            fs.mkdirs(destPath);
        }
        fs.copyFromLocalFile(new Path(srcFile),destPath);
        fs.close();
        System.out.println("success");
    }
    /**
     * 2、利用流api
     */
    @Test
    public void testUpload() throws Exception {
        FileSystem fs = getFs();
        FileInputStream inputStream = new FileInputStream("I://msdia80.dll");//输入
        //输出 ，这种就不是放在/test2/目录下了，而是直接创建test2文件了
        FSDataOutputStream outputStream = fs.create(new Path("/test2/"), false);

        //注意这里的IOUtils是org.apache.hadoop.io.IOUtils中的包，否则创建不成功
        IOUtils.copyBytes(inputStream, outputStream, 4096);

        fs.close();
    }

    /**
     * 流式下载
     * @throws Exception
     */
    @Test
    public void testDownload() throws Exception{
        FileSystem fs = getFs();
        FSDataInputStream fin = fs.open(new Path("/test2"));

        FileOutputStream out = new FileOutputStream("I://msdia80.dll_BAK");
        IOUtils.copyBytes(fin, out, 4096);
        System.out.println("over");
        fs.close();
    }

    @Test
    public void testMkdir() throws Exception{
        FileSystem fs = getFs();

        boolean flag = fs.mkdirs(new Path("/xuegod"));
        System.out.println(flag);
    }

    @Test
    public void testDel() throws Exception{
        FileSystem fs = getFs();
//		boolean flag = fs.delete(new Path("/ip.txt"));
        boolean flag = fs.deleteOnExit(new Path("/user/bigdata/result"));
        System.out.println(flag);
    }
}
