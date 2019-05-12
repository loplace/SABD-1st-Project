package queries;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import parser.validators.TemperatureValidator;
import utils.locationinfo.CityAttributesPreprocessor;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class SecondQuerySolver {

    public static void main(String[] args) throws IOException {


        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Second query Solver");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");


        // Load and parse data
        //String path = args[0];
        String pathTemperature = "/home/federico/Scaricati/prj1_dataset/temperature.csv";
        String pathPressure = "/home/federico/Scaricati/prj1_dataset/pressure.csv";
        String pathHumidity = "/home/federico/Scaricati/prj1_dataset/humidity.csv";

        /*
        String pathTemperature = "/Users/antonio/Downloads/prj1_dataset/temperature.csv";
        String pathPressure = "/Users/antonio/Downloads/prj1_dataset/pressure.csv";
        String pathHumidity = "/Users/antonio/Downloads/prj1_dataset/humidity.csv"; */

        TemperatureValidator temperatureValidator = new TemperatureValidator();
        temperatureValidator.preprocessTemperature(pathTemperature);

        Reader temperatureReader = null;
        Reader pressionReader = null;
        Reader humidityReader = null;

        try {
            temperatureReader = new FileReader(pathTemperature);
            pressionReader = new FileReader(pathPressure);
            humidityReader = new FileReader(pathHumidity);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }

        //new, use preprocessor to grab city ID for correct UTC
        CityAttributesPreprocessor cityAttributesPreprocessor = new CityAttributesPreprocessor();
        Map<String, CityModel> cities = cityAttributesPreprocessor.process().getCities();

        List<WeatherMeasurementPojo> humidities = csvToMeasurementPojo(humidityReader,cities);
        List<WeatherMeasurementPojo> pressures = csvToMeasurementPojo(pressionReader,cities);
        List<WeatherMeasurementPojo> temperatures = csvToMeasurementPojo(temperatureReader,cities);

        final double start = System.nanoTime();

        JavaRDD<WeatherMeasurementPojo> humiditiesRDD = jsc.parallelize(humidities,850);
        JavaRDD<WeatherMeasurementPojo> temperaturesRDD = jsc.parallelize(temperatures,850);
        JavaRDD<WeatherMeasurementPojo> pressuresRDD = jsc.parallelize(pressures,850);

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

    private static List<WeatherMeasurementPojo> csvToMeasurementPojo(Reader in, Map<String, CityModel> cities) throws IOException {
        List<WeatherMeasurementPojo> measurements = new ArrayList<>();
        Iterable<CSVRecord> records;
        Set<String> headers;
        records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord(false).parse(in);
        headers = records.iterator().next().toMap().keySet();
        headers.remove("datetime");

//        headers.forEach(System.out::println);
        Iterator<CSVRecord> iterator = records.iterator();
        while (iterator.hasNext()) {

            CSVRecord record = iterator.next();
            for (String field : headers) {

                String dateTime = record.get("datetime");
                String measurementValue = record.get(field);


                if (!measurementValue.isEmpty() && !dateTime.isEmpty()) {

                    double measurement = Double.parseDouble(measurementValue);
                    WeatherMeasurementPojo wmp = new WeatherMeasurementPojo(field, dateTime, measurement);

                    String key = wmp.getCity();
                    CityModel cityModel = cities.get(key);
                    if (cityModel != null){
                        String country = cityModel.getCountry();
                        wmp.setCountry(country);
                    }

                    measurements.add(wmp);

                }
            }

        }
        return  measurements;
    }


}
