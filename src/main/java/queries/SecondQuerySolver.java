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
import parser.WeatherRDDLoader;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.configuration.AppConfiguration;
import utils.locationinfo.CityAttributesPreprocessor;
import java.util.List;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
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

     //   TemperatureValidator temperatureValidator = new TemperatureValidator();
     //   temperatureValidator.preprocessTemperature(pathTemperature);

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
        Map<String, CityModel> cities = new CityAttributesPreprocessor().process().getCities();

    /*    List<WeatherMeasurementPojo> humidities = csvToMeasurementPojo(humidityReader,cities);
        List<WeatherMeasurementPojo> pressures = csvToMeasurementPojo(pressionReader,cities);
        List<WeatherMeasurementPojo> temperatures = csvToMeasurementPojo(temperatureReader,cities);

        JavaRDD<WeatherMeasurementPojo> humiditiesRDD = jsc.parallelize(humidities,850);
        JavaRDD<WeatherMeasurementPojo> temperaturesRDD = jsc.parallelize(temperatures,850);
        JavaRDD<WeatherMeasurementPojo> pressuresRDD = jsc.parallelize(pressures,850);


        */
        JavaRDD<WeatherMeasurementPojo> humiditiesRDD = new WeatherRDDLoader(cities)
                .loadWeatherMeasurementPojoRDDFromFile(pathHumidity);

        JavaRDD<WeatherMeasurementPojo> pressuresRDD = new WeatherRDDLoader(cities)
                .loadWeatherMeasurementPojoRDDFromFile(pathPressure);

        JavaRDD<WeatherMeasurementPojo> temperaturesRDD = new WeatherRDDLoader(cities)
                .loadWeatherMeasurementPojoRDDFromFile(pathTemperature);

        /*JavaRDD<String> csvData = jsc.textFile(pathHumidity);
        String headerHumidity = csvData.first();
        JavaRDD<String> noheaderRDD = csvData.filter(row -> !row.equals(headerHumidity));

        JavaRDD<WeatherMeasurementPojo> humiditiesRDD = noheaderRDD.flatMap(
                        new WeatherMeasurementParserFlatMap(headerHumidity).setCitiesMap(cities)
                );

        JavaRDD<String> csvPressureData = jsc.textFile(pathPressure);
        String headerPressure = csvPressureData.first();
        JavaRDD<String> noheaderPressureRDD = csvPressureData.filter(row -> !row.equals(headerPressure));

        JavaRDD<WeatherMeasurementPojo> pressuresRDD = noheaderPressureRDD.flatMap(
                new WeatherMeasurementParserFlatMap(headerPressure).setCitiesMap(cities)
        );

        JavaRDD<String> csvTemperatureData = jsc.textFile(pathTemperature);
        String headerTemperature = csvTemperatureData.first();
        JavaRDD<String> noheaderTemperatureRDD = csvTemperatureData.filter(row -> !row.equals(headerTemperature));

        JavaRDD<WeatherMeasurementPojo> temperaturesRDD = noheaderTemperatureRDD.flatMap(
                new WeatherMeasurementParserFlatMap(headerTemperature).setCitiesMap(cities)
        );
*/

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

    /*private static List<WeatherMeasurementPojo> csvToMeasurementPojo(Reader in, Map<String, CityModel> cities) throws IOException {
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
*/

}
