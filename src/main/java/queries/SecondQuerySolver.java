package queries;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import parser.WeatherRDDLoaderFromTextFile;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.configuration.AppConfiguration;
import utils.locationinfo.CityAttributesPreprocessor;

import java.io.IOException;
import java.util.*;

public class SecondQuerySolver {

    public static void main(String[] args) throws IOException {


        /*SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Second query Solver");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");*/


        // Load and parse data
        //String path = args[0];

        String pathTemperature = AppConfiguration.getProperty("dataset.csv.temperature");
        String pathPressure = AppConfiguration.getProperty("dataset.csv.pressure");
        String pathHumidity = AppConfiguration.getProperty("dataset.csv.humidity");


        //new, use preprocessor to grab city ID for correct UTC
        Map<String, CityModel> cities = new CityAttributesPreprocessor().process().getCities();

        /*
        List<WeatherMeasurementPojo> humidities = WeatherMeasurementCSVParser.csvToMeasurementPojo(pathHumidity,cities);
        List<WeatherMeasurementPojo> pressures = WeatherMeasurementCSVParser.csvToMeasurementPojo(pathPressure,cities);
        List<WeatherMeasurementPojo> temperatures = WeatherMeasurementCSVParser.csvToMeasurementPojo(pathTemperature,cities);

        JavaRDD<WeatherMeasurementPojo> humiditiesRDD = jsc.parallelize(humidities,850);
        JavaRDD<WeatherMeasurementPojo> temperaturesRDD = jsc.parallelize(temperatures,850);
        JavaRDD<WeatherMeasurementPojo> pressuresRDD = jsc.parallelize(pressures,850);
        */

        JavaRDD<WeatherMeasurementPojo> humiditiesRDD = new WeatherRDDLoaderFromTextFile(cities)
                .loadWeatherMeasurementPojoRDD(pathHumidity);

        JavaRDD<WeatherMeasurementPojo> pressuresRDD = new WeatherRDDLoaderFromTextFile(cities)
                .loadWeatherMeasurementPojoRDD(pathPressure);

        JavaRDD<WeatherMeasurementPojo> temperaturesRDD = new WeatherRDDLoaderFromTextFile(cities)
                .loadWeatherMeasurementPojoRDD(pathTemperature);


        final double start = System.nanoTime();

        JavaPairRDD<Tuple3<String, Integer, Integer>, Tuple4<Double, Double, Double, Double>> humiditiesOutput = computeAggregateValuesFromRDD(humiditiesRDD);
        JavaPairRDD<Tuple3<String, Integer, Integer>, Tuple4<Double, Double, Double, Double>> temperaturesOutput = computeAggregateValuesFromRDD(temperaturesRDD);
        JavaPairRDD<Tuple3<String, Integer, Integer>, Tuple4<Double, Double, Double, Double>> pressuresOutput = computeAggregateValuesFromRDD(pressuresRDD);

        final double end = System.nanoTime();

        final double delta = (end - start)/1000000000L;

        System.out.printf("Query 2 completed in %f seconds\n",delta);

        printAggregateValues("Humidity",humiditiesOutput);
        printAggregateValues("Temperature",temperaturesOutput);
        printAggregateValues("Pressure",pressuresOutput);


    }

    private static void printAggregateValues(String aggregateDatasetName, JavaPairRDD<Tuple3<String, Integer, Integer>, Tuple4<Double, Double, Double, Double>> aggregateOutput) {
        System.out.println("\nAggregate results for: "+aggregateDatasetName);
        System.out.println("Country\t Year\t Month\t Mean\t DevStd\t Min\t Max\t ");
        aggregateOutput.collect().forEach(x -> {
            Tuple3<String, Integer, Integer> key = x._1();
            Tuple4<Double, Double, Double, Double> t = x._2;
            System.out.printf("%s\t\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\n",
                    key._1(),key._2(),key._3(),t._1(),t._2(),t._3(),t._4());
        });
    }

    private static JavaPairRDD<Tuple3<String, Integer, Integer>, Tuple4<Double, Double, Double, Double>>
                        computeAggregateValuesFromRDD(JavaRDD<WeatherMeasurementPojo> measurementPojoJavaRDD) {
        // Chiave: country,year,month
        // Valore: Double della misurazione
        JavaPairRDD<Tuple3<String,Integer,Integer>, Double> keyedByCountryRDD = measurementPojoJavaRDD.mapToPair(
                wmp -> {
                 String country = wmp.getCountry();
                 Integer year = wmp.getMeasuredAt().getYear();
                 Integer month = wmp.getMeasuredAt().getMonthOfYear();
                 Tuple3<String,Integer,Integer> tupleKey = new Tuple3<>(country,year,month);

                 return new Tuple2<>(tupleKey,wmp.getMeasurementValue());
                });

        return keyedByCountryRDD.aggregateByKey(
                new StatCounter(),
                (acc, x) -> acc.merge(x),
                (acc1, acc2) -> acc1.merge(acc2)
            )
            .mapToPair(x -> {
                Tuple3<String,Integer,Integer> key = x._1();
                Double mean = x._2().mean();
                Double std = x._2().popStdev();
                Double min = x._2().min();
                Double max = x._2().max();

                Tuple4<Double, Double,Double, Double> value = new Tuple4<>(mean,std,min,max);
                return new Tuple2<>(key,value);
            });
    }

}
