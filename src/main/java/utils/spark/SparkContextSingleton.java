package utils.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkContextSingleton {

    private static SparkContextSingleton instance;
    private JavaSparkContext sc;
    private SparkSession spark;

    public static SparkContextSingleton getMe(String local) {

        if (instance == null) {

            instance = new SparkContextSingleton(local);
            return instance;

        }
        return instance;
    }


    private SparkContextSingleton(String local)
    {

        SparkConf conf = new SparkConf().setAppName("Query").set("spark.hadoop.validateOutputSpecs", "false");

        if (local.equals("local"))
            conf.setMaster("local[*]");

        sc = new JavaSparkContext(conf);

    }

    public JavaSparkContext getContext() {
        return sc;
    }
}
