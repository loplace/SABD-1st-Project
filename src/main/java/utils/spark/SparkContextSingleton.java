package utils.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import utils.configuration.AppConfiguration;

public class SparkContextSingleton {

    private static SparkContextSingleton instance;
    private JavaSparkContext jsc;

    public static SparkContextSingleton getInstance() {
        String sec = AppConfiguration.getSparkExecuteContext();
        if (instance == null) {
            instance = new SparkContextSingleton(sec);
            return instance;
        }
        return instance;
    }


    private SparkContextSingleton(String local) {
        String appName = AppConfiguration.getApplicationName();
        SparkConf conf = new SparkConf().setAppName(appName).set("spark.hadoop.validateOutputSpecs", "false");

        if (local.equals("local")) {
            conf.setMaster("local[*]");
        }
        jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
    }

    public JavaSparkContext getContext() {
        return jsc;
    }
}
