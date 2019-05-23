package queries;

import model.CityModel;
import model.WeatherDescriptionPojo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.configuration.AppConfiguration;
import utils.hdfs.HDFSDataLoader;
import utils.hdfs.HDFSHelper;
import utils.kafka.MyKafkaProducer;
import utils.locationinfo.CityAttributesPreprocessor;
import utils.spark.SparkContextSingleton;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static utils.hdfs.HDFSDataLoader.DATASETNAME.WEATHER_DESC;

public class FirstQuerySolver {

    public final static int STATICONE = 1;


    public static void main(String args[]) {

        String sparkExecContext = args[0]; //local or cluster
        String fileFormat = args[1]; //csv, parquet, read from kafka topic
        String appName = args[2]; // app name


        AppConfiguration
                .setSparkExecutionContext(sparkExecContext)
                .setApplicationName(appName);

        JavaSparkContext jsc = SparkContextSingleton.getInstance().getContext();


        LocalTime startHour = new LocalTime("08:00:00");
        LocalTime endHour = new LocalTime("18:00:00");


        // Load and parse data
        HDFSDataLoader.setFileFormat(fileFormat);
        String pathDescription = HDFSDataLoader.getDataSetFilePath(WEATHER_DESC);
        System.out.println("fileFormat: "+fileFormat);
        System.out.println("pathDescription: "+pathDescription);

        Integer[] months = {3,4,5};
        List<Integer> selectedMonths = Arrays.asList(months);

        //Grab city ID for correct UTC and nation
        CityAttributesPreprocessor cityAttributesPreprocessor = new CityAttributesPreprocessor();

        Map<String, CityModel> cities = cityAttributesPreprocessor.process().getCities();

        HDFSDataLoader.setCityMap(cities);

        final double start = System.nanoTime();

        //Create RDD containing pojos of weather description
        JavaRDD<WeatherDescriptionPojo> descriptionRDD = HDFSDataLoader.loadWeatherDescriptiontPojo(pathDescription);

        //Custom filter to grab only pojos where month is March, April and May
        Function<WeatherDescriptionPojo,Boolean> filterMarchAprilMay = e-> selectedMonths.contains(e.getLocalDateTime().getMonthOfYear());

        //apply filter for months
        JavaPairRDD<Integer, Iterable<String>> resultRDD = descriptionRDD.filter(filterMarchAprilMay)
                //take only pojos where description is "sky is clear
                .filter(wdp -> wdp.getWeatherCondition().equals("sky is clear"))

                //take pojos during daylight, meaning from 08:00 to 18:00
                .filter(wdp -> wdp.getLocalDateTime().hourOfDay().compareTo(startHour) >=0 &&
                        wdp.getLocalDateTime().hourOfDay().compareTo(endHour)<=0 )

                // key = Tuple4<Year,City,Month,Day of the month>, value =1
                .mapToPair( wdp ->
                        new Tuple2<>(new Tuple4<> (wdp.getLocalDateTime().getYear(),
                                wdp.getCity(), wdp.getLocalDateTime().getMonthOfYear(),
                                wdp.getLocalDateTime().getDayOfMonth()),STATICONE))

                //count number of hours of "sky is clear" during daylight
                .reduceByKey((a, b) -> a+b)

                //Take only records that have at least 8 hours of "Sky is clear" during daylight
                .filter(wdp-> wdp._2() >= 8)

                // key = Tuple3<Year, city, month> , value =1
                .mapToPair(wdp -> {
                    Tuple4<Integer, String, Integer, Integer> oldK = wdp._1();
                    Tuple3<Integer, String, Integer> newK = new Tuple3<>(oldK._1(),oldK._2(),oldK._3());
                    return new Tuple2<>(newK,STATICONE);
                })

                //Count how many days have 8 hours of sky is clear during daylight
                .reduceByKey((a,b) -> a+b)

                //Take only records that have at least 15 days with 8 hours of sky is clear during daylight
                .filter(wdp-> wdp._2() >= 15)

                // key = Tuple2<Year,City>, value is 1
                .mapToPair(wdp -> {

                    Tuple3<Integer, String, Integer> oldK = wdp._1();
                    Tuple2<Integer,String> newK = new Tuple2<>(oldK._1(),oldK._2());

                    return new Tuple2<>(newK,STATICONE);
                })

                //Count how many months have  at least 15 days with 8 hours of sky is clear during daylight
                .reduceByKey((a,b) -> a+b)

                //Take only records that have 15 days... in March && April && May
                .filter(wdp-> wdp._2() == 3)

                //Key = Year, Value = City that satisfied all the requisites
                .mapToPair(wdp-> new Tuple2<>(wdp._1()._1(),wdp._1()._2()))
                .groupByKey();


        final double end = System.nanoTime();

        final double delta = (end - start)/1000000000L;

        resultRDD.repartition(1).saveAsTextFile(AppConfiguration.getProperty("outputresults.query1"));

        System.out.printf("Query 1 completed in %f seconds\n",delta);

    }

}

