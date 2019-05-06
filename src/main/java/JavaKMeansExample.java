import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


import utils.CSVtoCity;
import utils.CityModel;


/**
 * KMeans Classification using spark MLlib in Java
 *
 */
public class JavaKMeansExample {
    public static void main(String[] args) {

        System.out.println("KMeans Classification using spark MLlib in Java . . .");

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Load and parse data
        String path = "/home/federico/Scaricati/prj1_dataset/city_attributes.csv";
        JavaRDD data = jsc.textFile(path);
        JavaRDD<CityModel> parsedData = (JavaRDD<CityModel>) data.map(line -> CSVtoCity.mapCSV( (String) line));

        JavaRDD result = parsedData.map (x -> {
            String[] sarray = {x.getLatitude(),x.getLongitude()};
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });

        // Cluster the data into three classes using KMeans
        int numClusters = 2;
        int numIterations = 5;
        KMeansModel clusters = KMeans.train(result.rdd(), numClusters, numIterations);




        System.out.println("\n*****Training*****");
        int clusterNumber = 0;
        for (Vector center: clusters.clusterCenters()) {
            System.out.println("Cluster center for Cluster "+ (clusterNumber++) + " : " + center);
        }
        JavaRDD cluster_indices = clusters.predict(result);

        for(Object line:cluster_indices.collect()){
            System.out.println("* "+line);
        }

        double cost = clusters.computeCost(result.rdd());
        System.out.println("\nCost: " + cost);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(result.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        try {
            FileUtils.forceDelete(new File("KMeansModel"));
            System.out.println("\nDeleting old model completed.");
        } catch (FileNotFoundException e1) {
        } catch (IOException e) {
        }

        // Save and load model
        clusters.save(jsc.sc(), "KMeansModel");
        System.out.println("\rModel saved to KMeansModel/");
        KMeansModel sameModel = KMeansModel.load(jsc.sc(),
                "KMeansModel");

        jsc.stop();
    }
}