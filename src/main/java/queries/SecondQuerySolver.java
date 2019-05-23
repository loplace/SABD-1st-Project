package queries;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import parser.validators.HumidityValidator;
import parser.validators.PressureValidator;
import parser.validators.TemperatureValidator;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.configuration.AppConfiguration;
import utils.hdfs.HDFSDataLoader;
import utils.locationinfo.CityAttributesPreprocessor;
import utils.spark.SparkContextSingleton;

import java.util.Map;

import static utils.hdfs.HDFSDataLoader.DATASETNAME.*;

public class SecondQuerySolver {

    public static void main(String[] args) {

        String sparkExecContext = args[0];
        String fileFormat = args[1];
        String appName = args[2]; // app name

        AppConfiguration
                .setSparkExecutionContext(sparkExecContext)
                .setApplicationName(appName);
        JavaSparkContext jsc = SparkContextSingleton.getInstance().getContext();

        System.out.println("sparkExecContext: "+sparkExecContext);
        System.out.println("fileFormat: "+fileFormat);

        HDFSDataLoader.setFileFormat(fileFormat);
        String pathTemperature = HDFSDataLoader.getDataSetFilePath(TEMPERATURE);
        String pathPressure = HDFSDataLoader.getDataSetFilePath(PRESSURE);
        String pathHumidity = HDFSDataLoader.getDataSetFilePath(HUMIDITY);

        System.out.println("pathTemperature: "+pathTemperature);
        System.out.println("pathPressure: "+pathPressure);
        System.out.println("pathHumidity: "+pathHumidity);

        final double start = System.nanoTime();

        // preprocessor to grab city ID for correct UTC and city nations
        Map<String, CityModel> cities = new CityAttributesPreprocessor().process().getCities();

        //Broadcast<Map<String, CityModel>> mapBroadcast = jsc.broadcast(cities);
        HDFSDataLoader.setCityMap(cities);

        //Custom validators to check if data are reasonable (i.e. a temperature of thousands Kelvin)
        JavaRDD<WeatherMeasurementPojo> humidityRDD =
                HDFSDataLoader.loadWeatherMeasurementPojo(pathHumidity,new HumidityValidator());

        JavaRDD<WeatherMeasurementPojo> temperatureRDD =
                HDFSDataLoader.loadWeatherMeasurementPojo(pathTemperature, new TemperatureValidator());

        JavaRDD<WeatherMeasurementPojo> pressureRDD =
                HDFSDataLoader.loadWeatherMeasurementPojo(pathPressure, new PressureValidator());

        //For each dataset, create RDD with Key = Tuple3<Country, Year, Month>, Value = Tuple4<Mean,Std, Min, Max>
        JavaPairRDD<Tuple3<String, Integer, Integer>, Tuple4<Double, Double, Double, Double>> humiditiesOutput = computeAggregateValuesFromRDD(humidityRDD);
        JavaPairRDD<Tuple3<String, Integer, Integer>, Tuple4<Double, Double, Double, Double>> temperaturesOutput = computeAggregateValuesFromRDD(temperatureRDD);
        JavaPairRDD<Tuple3<String, Integer, Integer>, Tuple4<Double, Double, Double, Double>> pressuresOutput = computeAggregateValuesFromRDD(pressureRDD);

        final double end = System.nanoTime();

        final double delta = (end - start)/1000000000L;

        System.out.printf("Query 2 completed in %f seconds\n",delta);

        humiditiesOutput.repartition(1).saveAsTextFile(AppConfiguration.getProperty("outputresults.query2")+"humidities");
        temperaturesOutput.repartition(1).saveAsTextFile(AppConfiguration.getProperty("outputresults.query2")+"temperatures");
        pressuresOutput.repartition(1).saveAsTextFile(AppConfiguration.getProperty("outputresults.query2")+"pressures");



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

        // Key: Tuple3<country,year,month> , Value = measurement value
        JavaPairRDD<Tuple3<String,Integer,Integer>, Double> keyedByCountryRDD = measurementPojoJavaRDD.mapToPair(
                wmp -> {
                 String country = wmp.getCountry();
                 Integer year = wmp.getMeasuredAt().getYear();
                 Integer month = wmp.getMeasuredAt().getMonthOfYear();
                 Tuple3<String,Integer,Integer> tupleKey = new Tuple3<>(country,year,month);

                 return new Tuple2<>(tupleKey,wmp.getMeasurementValue());
                });

        //Use statcounter to compute aggregate values (mean, std, min, max),
        return keyedByCountryRDD.aggregateByKey(
                new StatCounter(),
                (acc, x) -> acc.merge(x),
                (acc1, acc2) -> acc1.merge(acc2)
            )

        //Key = Tuple3<Country, year, month>, Value = Tuple4<mean, std, min, max>
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
