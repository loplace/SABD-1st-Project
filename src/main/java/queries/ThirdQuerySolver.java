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
import org.joda.time.LocalTime;
import parser.WeatherMeasurementParserFlatMap;
import parser.WeatherRDDLoader;
import scala.Tuple2;
import utils.configuration.AppConfiguration;
import utils.locationinfo.CityAttributesPreprocessor;

import java.io.*;
import java.util.*;

public class ThirdQuerySolver {

    private final static Integer[] hotMonths = {6,7,8,9};
    public final static  List<Integer> hotMonthsList = Arrays.asList(hotMonths);
    private final static Integer[] coldMonths = {1,2,3,4};
    public final static List<Integer> coldMonthsList = Arrays.asList(coldMonths);

    public final static LocalTime startHour = new LocalTime("12:00:00");
    public final static LocalTime endHour = new LocalTime("15:00:00");

    public static void main(String[] args) {


       /* SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("First query Solver");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");*/

        // Load and parse data
        String pathTemperature = AppConfiguration.getProperty("dataset.csv.temperature");

        //new, use preprocessor to grab city ID for correct UTC
        Map<String, CityModel> cities = new CityAttributesPreprocessor().process().getCities();

        /*JavaRDD<String> csvTemperatureData = jsc.textFile(pathTemperature);
        String headerTemperature = csvTemperatureData.first();
        JavaRDD<String> noheaderTemperatureRDD = csvTemperatureData.filter(row -> !row.equals(headerTemperature));

        JavaRDD<WeatherMeasurementPojo> temperaturesRDD = noheaderTemperatureRDD.flatMap(
                new WeatherMeasurementParserFlatMap(headerTemperature).setCitiesMap(cities)
        );
*/
        JavaRDD<WeatherMeasurementPojo> temperaturesRDD = new WeatherRDDLoader(cities).loadWeatherMeasurementPojoRDDFromFile(pathTemperature);

       // List<WeatherMeasurementPojo> temperatures = csvToMeasurementPojo(temperatureReader,cities);

        final double start = System.nanoTime();

       // JavaRDD<WeatherMeasurementPojo> temperaturesRDD = jsc.parallelize(temperatures,850);

        JavaRDD<WeatherMeasurementPojo> tempPer2016RDD = temperaturesRDD.filter(x -> x.getMeasuredAt().getYear()==2016);
        JavaRDD<WeatherMeasurementPojo> tempPer2017RDD = temperaturesRDD.filter(x -> x.getMeasuredAt().getYear()==2017);

        JavaPairRDD<Tuple2<String, Double>, Long> descRankByMeanDiff2016 = createRankByHotNColdAvgDiff(tempPer2016RDD);
        JavaPairRDD<Tuple2<String, Double>, Long> descRankByMeanDiff2017 = createRankByHotNColdAvgDiff(tempPer2017RDD);

        JavaPairRDD<Tuple2<String, Double>, Long> top3of2017RDD = descRankByMeanDiff2017.filter(x -> x._2() <= 2);

        JavaPairRDD<String, Tuple2<Double, Long>> res2017 = top3of2017RDD.mapToPair(x -> {
            String key = x._1()._1();
            Tuple2<Double, Long> value = new Tuple2<>(x._1()._2(), x._2 + 1);
            return new Tuple2<>(key, value);
        });

        JavaPairRDD<String, Tuple2<Double, Long>> res2016 = descRankByMeanDiff2016.mapToPair(x -> {
            String key = x._1()._1();
            Tuple2<Double, Long> value = new Tuple2<>(x._1()._2(), x._2+1);
            return new Tuple2<>(key, value);
        });

        final double end = System.nanoTime();
        final double delta = (end - start)/1000000000L;
        System.out.printf("Query 3 completed in %f seconds\n",delta);
        System.out.println("Top 3 of 2017 with relative position in 2016");
        List<Tuple2<String, Tuple2<Tuple2<Double, Long>, Tuple2<Double, Long>>>> finalResultSet = res2017.join(res2016).collect();

        printResultSet(finalResultSet);

    }

    private static void printResultSet(List<Tuple2<String, Tuple2<Tuple2<Double, Long>, Tuple2<Double, Long>>>> finalResultSet) {
        System.out.println("City\t Abs Mean Diff 2017 \t Pos 2017\t Abs Mean Diff 2017 \t Pos 2017\t");
        finalResultSet.forEach(x -> {
            String city = x._1;
            double val2017 = x._2()._1()._1();
            long pos2017 = x._2()._1()._2();
            double val2016 = x._2()._2()._1();
            long pos2016 = x._2()._2()._2();
            System.out.printf("%s\t%f\t%d\t%f\t%d\n",city,val2017,pos2017,val2016,pos2016);
        });
    }

    private static JavaPairRDD<Tuple2<String, Double>, Long> createRankByHotNColdAvgDiff(JavaRDD<WeatherMeasurementPojo> tempPerYearRDD) {
        JavaRDD<WeatherMeasurementPojo> filteredLocalTimeTemps = tempPerYearRDD.filter(wdp -> wdp.getMeasuredAt().hourOfDay().compareTo(startHour) >= 0 &&
                wdp.getMeasuredAt().hourOfDay().compareTo(endHour) <= 0);

        JavaRDD<WeatherMeasurementPojo> hotMonthsTempsRDD = filteredLocalTimeTemps.filter(
                wmp -> hotMonthsList.contains(wmp.getMeasuredAt().getMonthOfYear()));
        JavaRDD<WeatherMeasurementPojo> coldMonthsTempsRDD = filteredLocalTimeTemps.filter(
                wmp -> coldMonthsList.contains(wmp.getMeasuredAt().getMonthOfYear()));

        JavaPairRDD<String,Double> avgTempHotMonths = hotMonthsTempsRDD.mapToPair(
                wmp -> new Tuple2<>(wmp.getCity(),wmp.getMeasurementValue())
            ).aggregateByKey(new StatCounter(),
                (acc, x) -> acc.merge(x),
                (acc1, acc2) -> acc1.merge(acc2)
            ).mapToPair(x -> {
                String key = x._1();
                Double mean = x._2().mean();
                return new Tuple2<>(key,mean);
            });

        JavaPairRDD<String,Double> avgTempColdMonths = coldMonthsTempsRDD.mapToPair(
                wmp -> new Tuple2<>(wmp.getCity(),wmp.getMeasurementValue())
        ).aggregateByKey(new StatCounter(),
                (acc, x) -> acc.merge(x),
                (acc1, acc2) -> acc1.merge(acc2)
        ).mapToPair(x -> {
            String key = x._1();
            Double mean = x._2().mean();
            return new Tuple2<>(key,mean);
        });

        JavaPairRDD<String, Tuple2<Double, Double>> join2016 = avgTempHotMonths.join(avgTempColdMonths);

        JavaPairRDD<String, Double> meanDiff2016 = join2016.mapToPair(x -> new Tuple2<>(x._1(), Math.abs(x._2()._1() - x._2()._2())));

        JavaPairRDD<String, Double> descRankByMeanDiff = meanDiff2016.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());

        return descRankByMeanDiff.zipWithIndex();
    }


    /*private static List<WeatherMeasurementPojo> csvToMeasurementPojo(Reader in, Map<String, CityModel> cities) throws IOException {
        List<WeatherMeasurementPojo> measurements = new ArrayList<>();
        Iterable<CSVRecord> records;
        Set<String> headers;
        records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord(false).parse(in);
        headers = records.iterator().next().toMap().keySet();
        headers.remove("datetime");

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
    }*/


}
