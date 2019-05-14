package utils.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import utils.configuration.AppConfiguration;

public class SparkContextSingleton {

    private static SparkContextSingleton instance;
    private JavaSparkContext sc;

    public static SparkContextSingleton getInstance() {
        String sec = AppConfiguration.getSparkExecuteContext();
        if (instance == null) {
            instance = new SparkContextSingleton(sec);
            return instance;
        }
        return instance;
    }


    private SparkContextSingleton(String local) {
        SparkConf conf = new SparkConf().setAppName("Query").set("spark.hadoop.validateOutputSpecs", "false");

        if (local.equals("local")) {
            conf.setMaster("local[*]");
        }
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
    }

    public JavaSparkContext getContext() {
        return sc;
    }
}
