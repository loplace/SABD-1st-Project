package queries;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.StatCounter;
import org.joda.time.LocalTime;
import parser.validators.TemperatureValidator;
import queries.combiner.ARankCombiner;
import queries.combiner.CompleteCombiner;
import queries.combiner.TruncatedCombiner;
import scala.Tuple2;
import scala.Tuple3;
import utils.configuration.AppConfiguration;
import utils.hdfs.HDFSDataLoader;
import utils.locationinfo.CityAttributesPreprocessor;
import utils.spark.SparkContextSingleton;

import java.util.*;


import static utils.hdfs.HDFSDataLoader.DATASETNAME.TEMPERATURE;

public class ThirdQuerySolver {


    private final static Integer[] hotMonths = {6,7,8,9};
    public final static  List<Integer> hotMonthsList = Arrays.asList(hotMonths);
    private final static Integer[] coldMonths = {1,2,3,4};
    public final static List<Integer> coldMonthsList = Arrays.asList(coldMonths);

    public final static LocalTime startHour = new LocalTime("12:00:00");
    public final static LocalTime endHour = new LocalTime("15:00:00");


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
        System.out.println("pathTemperature: "+pathTemperature);

        final double start = System.nanoTime();

        // preprocessor to grab city ID for correct UTC and city nation
        Map<String, CityModel> cities = new CityAttributesPreprocessor().process().getCities();

        //Broadcast<Map<String, CityModel>> mapBroadcast = jsc.broadcast(cities);
        HDFSDataLoader.setCityMap(cities);

        //Create RDD with pojos, passing a validator to check if data are reasonable (i.e. a temperature of thousands Kelvin)
        //RDD cache since multiple methods are applied
        JavaRDD<WeatherMeasurementPojo> temperaturesRDD =
                HDFSDataLoader.loadWeatherMeasurementPojo(pathTemperature,new TemperatureValidator()).cache();

        //Get measurements for 2016 and 2017
        JavaRDD<WeatherMeasurementPojo> tempPer2016RDD = temperaturesRDD.filter(x -> x.getMeasuredAt().getYear()==2016);
        JavaRDD<WeatherMeasurementPojo> tempPer2017RDD = temperaturesRDD.filter(x -> x.getMeasuredAt().getYear()==2017);

        // Key = Nation, value = <City,Diff>
        JavaPairRDD<String, Tuple3<String, Double, Integer>> descRankByMeanDiff2016 = createRankByHotNColdAvgDiff(tempPer2016RDD, new CompleteCombiner());

        JavaPairRDD<String, Tuple3<String, Double, Integer>> descRankByMeanDiff2017 = createRankByHotNColdAvgDiff(tempPer2017RDD, new TruncatedCombiner());

        JavaPairRDD<Tuple2, Tuple2> reKeyed2016 = descRankByMeanDiff2016.mapToPair(x -> {
            Tuple2<String, String> key = new Tuple2<>(x._1(), x._2()._1());
            Tuple2<Double, Integer> value = new Tuple2<>(x._2()._2(), x._2()._3());
            Tuple2<Tuple2, Tuple2> pair = new Tuple2<>(key, value);
            return pair;

        });

        JavaPairRDD<Tuple2, Tuple2> reKeyed2017 = descRankByMeanDiff2017.mapToPair(x-> {
            Tuple2<String,String> key = new Tuple2<>(x._1(),x._2()._1());
            Tuple2<Double,Integer> value = new Tuple2<>(x._2()._2(),x._2()._3());
            Tuple2<Tuple2,Tuple2> pair = new Tuple2<>(key,value);
            return pair;

        });

        JavaPairRDD<Tuple2, Tuple2<Tuple2, Tuple2>> finalResultRDD = reKeyed2017.join(reKeyed2016);
        finalResultRDD.repartition(1).saveAsTextFile(AppConfiguration.getProperty("outputresults.query3"));


        final double end = System.nanoTime();
        final double delta = (end - start)/1000000000L;
        System.out.printf("Query 3 completed in %f seconds\n",delta);

    }

    private static JavaPairRDD<String, Tuple3<String, Double, Integer>> createRankByHotNColdAvgDiff(JavaRDD<WeatherMeasurementPojo> tempPerYearRDD, ARankCombiner combiner) {

        //Takes measurements between 12:00 - 15:00
        JavaRDD<WeatherMeasurementPojo> filteredLocalTimeTemps = tempPerYearRDD.filter(wdp -> wdp.getLocalDateTime().hourOfDay().compareTo(startHour) >= 0 &&
                wdp.getLocalDateTime().hourOfDay().compareTo(endHour) <= 0);

        //Takes measurements in June, July, August, September
        JavaRDD<WeatherMeasurementPojo> hotMonthsTempsRDD = filteredLocalTimeTemps.filter(
                wmp -> hotMonthsList.contains(wmp.getMeasuredAt().getMonthOfYear()));

        //Takes measurements in January, February, March, April
        JavaRDD<WeatherMeasurementPojo> coldMonthsTempsRDD = filteredLocalTimeTemps.filter(
                wmp -> coldMonthsList.contains(wmp.getMeasuredAt().getMonthOfYear()));

        //Key =Tuple2<Nation,City> Value = Mean Measurement value
        JavaPairRDD<Tuple2<String, String>, Double> avgTempHotMonths = computeAvgTemps(hotMonthsTempsRDD);
        //Same as above for cold months
        JavaPairRDD<Tuple2<String, String>, Double> avgTempColdMonths = computeAvgTemps(coldMonthsTempsRDD);

        //join on key hot months and cold months
        JavaPairRDD<Tuple2<String,String>, Tuple2<Double, Double>> join2016 = avgTempHotMonths.join(avgTempColdMonths);

        //Compute difference of averages
        JavaPairRDD<Tuple2<String,String>, Double> meanDiff2016 = join2016.mapToPair(x ->
                new Tuple2<>(x._1(), Math.abs(x._2()._1() - x._2()._2())));

        JavaPairRDD<String,Tuple2<String,Double>> cityInValueRDD = meanDiff2016.mapToPair( x-> {

            String key = x._1()._1();
            String city = x._1()._2();

            Tuple2<String, Double> value = new Tuple2<>(city,x._2());
            Tuple2<String, Tuple2<String,Double>> record = new Tuple2(key,value);
            return record;
        });

        Function<Tuple2<String, Double>, List<Tuple2<String, Double>>> createAcc = combiner.createAccumulator();
        Function2<List<Tuple2<String, Double>>, Tuple2<String, Double>, List<Tuple2<String, Double>>> mergeValue = combiner.createMergeValue();
        Function2<List<Tuple2<String, Double>>, List<Tuple2<String, Double>>, List<Tuple2<String, Double>>> mergeCombiners = combiner.createMergeCombiner();

        //Key is Nation, value is List<City,diff>
        JavaPairRDD<String, List<Tuple2<String, Double>>> combinedAndSorted = cityInValueRDD.combineByKey(createAcc, mergeValue, mergeCombiners);

        //Key is Nation, value is Tuple2<City,Diff>
        JavaPairRDD<String,Tuple3<String,Double,Integer>> flattenedRDD = combinedAndSorted.flatMapToPair(x->{

            List<Tuple2<String, Tuple3<String,Double,Integer>>> pairs = new LinkedList<>();
            for (Tuple2 t: x._2()) {
                Tuple3<String,Double,Integer> value = new Tuple3(t._1(),t._2(),x._2().indexOf(t) +1);
                pairs.add(new Tuple2<>(x._1(), value));
            }

            return pairs.iterator();
        });

        //return RDD adding Position in ranking
        return flattenedRDD;
    }

    private static JavaPairRDD<Tuple2<String, String>, Double> computeAvgTemps(JavaRDD<WeatherMeasurementPojo> hotMonthsTempsRDD) {
        return hotMonthsTempsRDD.mapToPair(
                wmp -> new Tuple2<>(new Tuple2<>(wmp.getCountry(), wmp.getCity()), wmp.getMeasurementValue())
        )
                //Compute aggregate operations
                .aggregateByKey(new StatCounter(),
                        (acc, x) -> acc.merge(x),
                        (acc1, acc2) -> acc1.merge(acc2)
                )
                //creat key-value pairs
                .mapToPair(x -> {

                    Tuple2<String, String> key = x._1();
                    Double mean = x._2().mean();
                    return new Tuple2<>(key, mean);
                });
    }
}