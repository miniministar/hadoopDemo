package com.exercise.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.checkerframework.checker.units.qual.C;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {
    private static String uri = "hdfs://hadoop100:8020";
    private static String user = "root";
    private static String localPath = "D:\\test\\hadoop\\";

    public static void main(String[] args) throws Exception {
//        testMkdirs();
//        testCopyFromLocalFile();
//        testCopyToLocalFile();
//        testRename();
//        testDelete();
//        testListFiles();
        testListStatus();
    }

    private static void testListStatus() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            }else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
        fs.close();


    }

    private static void testListFiles() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("============="+ fileStatus.getPath() + "==================");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

            fs.close();
        }
    }

    private static void testDelete()throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
//        fs.delete(new Path("/xiyou/huaguoshan/meihouwang.log"), true);
        fs.delete(new Path("/xiyou"), true);
        fs.close();

    }

    private static void testRename() throws Exception{
        Configuration configuration =new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong.log"), new Path("/xiyou/huaguoshan/meihouwang.log"));
        fs.close();
    }

    private static void testCopyToLocalFile() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan/sunwukong.log"), new Path(localPath + "sunwukong2.log"), true);
        fs.close();
    }

    private static void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");

        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        fs.copyFromLocalFile(new Path(localPath + "sunwukong.log"), new Path("/xiyou/huaguoshan"));
        fs.close();

    }

    private static void testMkdirs() {
        try {
            // 1 获取文件系统
            Configuration configuration = new Configuration();

            FileSystem fs = FileSystem.get(new URI(uri), configuration, "root");
            // 2 创建目录
            fs.mkdirs(new Path("/xiyou/huaguoshan/"));
            // 3 关闭资源
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


}
