package apps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class HdfsApplication {
    public static void main(String[] args) throws IOException {
        // hdfs api 主要就以Filesystem为中心,
        // open为打开某个文件,返回一个data stream.
        // 另外还有就是path和configuration.
        // open得到的流可以被包装进Java的其他流,如Gzip压缩流等等.

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + "172.16.30.103:8020"); // 8020位NameNode的端口,9000位文件系统的端口.
        conf.set("dfs.support.append", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");


        FileSystem fileSystem = FileSystem.get(conf);
        // 可以得到某一个路径下的全部文件或文件夹信息
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("直接写hdfs文件系统的路径,以/开头"));

        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream fsDataInputStream = fileSystem.open(fileStatus.getPath());
            // 用Java流对其进行包装.
            GZIPInputStream gzipInputStream = new GZIPInputStream(fsDataInputStream);
        }
    }
}