package dev.flink.alluxio;


import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @fileName: AlluxioFileSystemClient.java
 * @description: alluxio fileSystem客户端
 * @author: huangshimin
 * @date: 2022/1/17 7:55 PM
 */
public class AlluxioFileSystemClient {
    public static void main(String[] args) throws IOException, AlluxioException {
        AlluxioURI path = new AlluxioURI("alluxio://java/api");
        FileSystem fileSystem = FileSystem.Factory.get();
        // 写文件配置
        CreateFilePOptions options = CreateFilePOptions.newBuilder()
                // 设置block的大小
                .setBlockSizeBytes(125 * Constants.MB)
                .setWriteType(WritePType.ASYNC_THROUGH)
                .build();

        // 读取配置
        OpenFilePOptions readOptions = OpenFilePOptions.newBuilder()
                .setReadType(ReadPType.CACHE)
                .build();
        // 创建文件
        FileOutStream out = fileSystem.createFile(path, options);
        out.write("zhangsan".getBytes(StandardCharsets.UTF_8));
        out.close();
    }
}
