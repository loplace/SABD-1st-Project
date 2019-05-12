package utils.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HDFSHelper {

    private static final String hdfsURL = "hdfs://master:543210";


    public static void readFromHDFS (String filepath){



    }

    public static void writeToHDFS(String filepath) throws IOException {

        filepath = hdfsURL.concat(filepath);
        Configuration configuration = new Configuration();
        URI toUri = URI.create(filepath);
        FileSystem fs = FileSystem.get(toUri,configuration);

        FSDataOutputStream out = fs.create(new Path(toUri));

        out.writeBytes(filepath);
        out.close();

    }

}
