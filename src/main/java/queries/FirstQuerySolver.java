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
import utils.timekeeper.TimeKeeper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static utils.hdfs.HDFSDataLoader.DATASETNAME.WEATHER_DESC;

public class FirstQuerySolver {

    public final static int STATICONE = 1;


    public static void main(String args[]) {

        TimeKeeper tk = new TimeKeeper();
        String sparkExecContext = args[0];
        String fileFormat = args[1];

        tk.startPhase("InitSparkContext");
        StringBuilder sb = new StringBuilder();

        AppConfiguration.setSparkExecutionContext(sparkExecContext);
        JavaSparkContext jsc = SparkContextSingleton.getInstance().getContext();

        tk.endPhase("InitSparkContext");

        LocalTime startHour = new LocalTime("08:00:00");
        LocalTime endHour = new LocalTime("18:00:00");


        // Load and parse data
        HDFSDataLoader.setFileFormat(fileFormat);
        String pathDescription = HDFSDataLoader.getDataSetFilePath(WEATHER_DESC);
        System.out.println("fileFormat: "+fileFormat);
        sb.append("fileFormat: "+fileFormat+"\n");
        System.out.println("pathDescription: "+pathDescription);
        sb.append("pathDescription: "+pathDescription+"\n");

        Integer[] months = {3,4,5};
        List<Integer> selectedMonths = Arrays.asList(months);

        //new, use preprocessor to grab city ID for correct UTC
        tk.startPhase("City Attributes Preprocessor");
        CityAttributesPreprocessor cityAttributesPreprocessor = new CityAttributesPreprocessor();

        Map<String, CityModel> cities = cityAttributesPreprocessor.process().getCities();

        HDFSDataLoader.setCityMap(cities);

        tk.endPhase("City Attributes Preprocessor");

        final double start = System.nanoTime();

        tk.startPhase("DescriptionRDD loading");
        JavaRDD<WeatherDescriptionPojo> descriptionRDD = HDFSDataLoader.loadWeatherDescriptiontPojo(pathDescription);
        tk.endPhase("DescriptionRDD loading");

        tk.startPhase("Executing Query 1");

        //prendo tutti i POJO che si riferiscono a Marzo, Aprile e Maggio
        Function<WeatherDescriptionPojo,Boolean> filterMarchAprilMay = e-> selectedMonths.contains(e.getLocalDateTime().getMonthOfYear());
        JavaPairRDD<Integer, Iterable<String>> resultRDD = descriptionRDD.filter(filterMarchAprilMay)
                .filter(wdp -> wdp.getWeatherCondition().equals("sky is clear"))
                .filter(wdp -> wdp.getLocalDateTime().hourOfDay().compareTo(startHour) >=0 &&
                        wdp.getLocalDateTime().hourOfDay().compareTo(endHour)<=0 )
                .mapToPair( wdp ->
                        new Tuple2<>(new Tuple4<> (wdp.getLocalDateTime().getYear(),
                                wdp.getCity(), wdp.getLocalDateTime().getMonthOfYear(),
                                wdp.getLocalDateTime().getDayOfMonth()),STATICONE))
                .reduceByKey((a, b) -> a+b)
                .filter(wdp-> wdp._2() >= 8)
                .mapToPair(wdp -> {
                    Tuple4<Integer, String, Integer, Integer> oldK = wdp._1();
                    Tuple3<Integer, String, Integer> newK = new Tuple3<>(oldK._1(),oldK._2(),oldK._3());
                    return new Tuple2<>(newK,STATICONE);
                })
                .reduceByKey((a,b) -> a+b)
                .filter(wdp-> wdp._2() >= 15)
                .mapToPair(wdp -> {

                    Tuple3<Integer, String, Integer> oldK = wdp._1();
                    Tuple2<Integer,String> newK = new Tuple2<>(oldK._1(),oldK._2());

                    return new Tuple2<>(newK,STATICONE);
                })
                .reduceByKey((a,b) -> a+b)
                .filter(wdp-> wdp._2() == 3)
                .mapToPair(wdp-> new Tuple2<>(wdp._1()._1(),wdp._1()._2()))
                .groupByKey();



        tk.endPhase("Executing Query 1");

        final double end = System.nanoTime();

        final double delta = (end - start)/1000000000L;

        resultRDD.repartition(1).saveAsTextFile(AppConfiguration.getProperty("outputresults.query1"));

        System.out.printf("Query 1 completed in %f seconds\n",delta);
        sb.append(String.format("Query 1 completed in %f seconds\n",delta));

        tk.startPhase("Collecting Query 1");
        //List<Tuple2<Integer, Iterable<String>>> list = resultRDD.collect();
        tk.endPhase("Collecting Query 1");



      /*  MyKafkaProducer mkp = new MyKafkaProducer();
        mkp.putQueryResultonKafka("query1results",null,list);
*/
        /*int count = 0;
        for (Tuple2 o: list) {
            count++;
            System.out.println(o._1());
            sb.append(o._1().toString()+"\n");
            System.out.println(o._2());
            sb.append(o._2().toString()+"\n");
        }
*/
        // ottengo i risultati
        String outPutResults = sb.toString();
        String execTimes= tk.getTableTimes();
        System.out.println(execTimes);

        HDFSHelper.getInstance().writeStringToHDFS("/output","query1_"+fileFormat+"_times.txt",execTimes);

    }

}

