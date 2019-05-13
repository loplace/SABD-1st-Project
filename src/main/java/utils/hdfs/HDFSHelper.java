package utils.hdfs;

import lombok.Getter;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import utils.configuration.AppConfiguration;

import java.io.*;
import java.net.URI;
import java.util.logging.Logger;

public class HDFSHelper {

    private static final String hdfsURL = AppConfiguration.getProperty("hdfs.uri");

    private static final Logger logger = Logger.getLogger("");

    private Configuration conf;

    private static HDFSHelper instance = null;

    @Getter
    private FileSystem fs;

    private HDFSHelper(){

        initHDFSFileSystemObject();

    }

    private void initHDFSFileSystemObject() {

        conf = new Configuration();
        conf.set("fs.defaultFS", hdfsURL);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS
        try {
            fs = FileSystem.get(URI.create(hdfsURL), conf);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
            logger.warning("IOException: " + e.getMessage());
        }

    }


    private void initSubFolder(String path){

        Path workingDir=fs.getWorkingDirectory();
        Path newFolderPath= new Path(path);
        try {
            if(!fs.exists(newFolderPath)) {
                // Create new Directory
                fs.mkdirs(newFolderPath);
                logger.info("Path "+path+" created.");
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
            logger.warning("IOException: " + e.getMessage());
        }

    }


    public static HDFSHelper getInstance() {
        if(instance == null)
            instance = new HDFSHelper();
        return instance;
    }

    public String readFromHDFS(String folderpath, String filename){

        logger.info("Beginning Read on HDFS");

        Path hdfsreadpath = new Path(folderpath + "/" + filename);
        //Init input stream
        FSDataInputStream inputStream = null;

        //Classical input stream usage
        String out= null;

        try {
            inputStream = fs.open(hdfsreadpath);
            out = SerializationUtils.deserialize(inputStream);
            inputStream.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Ending Read on HDFS");


        return out;
    }


    public Byte readByteFromHDFS(String folderpath, String filename){

        logger.info("Beginning Read on HDFS");

        Path hdfsreadpath = new Path(folderpath + "/" + filename);
        //Init input stream
        FSDataInputStream inputStream = null;

        //Classical input stream usage
        Byte out= null;

        try {
            inputStream = fs.open(hdfsreadpath);
            out = SerializationUtils.deserialize(inputStream);
            inputStream.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Ending Read on HDFS");


        return out;
    }

    public void writeToHDFS(String folderpath, String filename, Serializable obj) {

        logger.info("Beginning Write on HDFS");

        initSubFolder(folderpath);
        Path hdfsWritePath = new Path(folderpath + "/" + filename);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fs.create(hdfsWritePath);
            byte[] data = SerializationUtils.serialize(obj);

            fsDataOutputStream.write(data);

            fsDataOutputStream.close();
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
            logger.warning("IOException: " + e.getMessage());
        }

        logger.info("Ending Write on HDFS");


    }


    public static void main(String[] args) {

        String demoContent = "Ciao Mondo";

        HDFSHelper hdfsInstance = HDFSHelper.getInstance();

        hdfsInstance.writeToHDFS("/demofolder","HelloWorld.txt",demoContent);

        String s = hdfsInstance.readFromHDFS("/demofolder", "HelloWorld.txt");

        System.out.println(s);


    }


}
