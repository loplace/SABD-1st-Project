package queries;

import model.CityModel;
import model.WeatherDescriptionPojo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.joda.time.LocalTime;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.configuration.AppConfiguration;
import utils.hdfs.HDFSDataLoader;
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

        AppConfiguration.setSparkExecutionContext(sparkExecContext);
        JavaSparkContext jsc = SparkContextSingleton.getInstance().getContext();

        tk.endPhase("InitSparkContext");

        LocalTime startHour = new LocalTime("08:00:00");
        LocalTime endHour = new LocalTime("18:00:00");


        // Load and parse data
        HDFSDataLoader.setFileFormat(fileFormat);
        String pathDescription = HDFSDataLoader.getDataSetFilePath(WEATHER_DESC);
        System.out.println("fileFormat: "+fileFormat);
        System.out.println("pathDescription: "+pathDescription);

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
        /*Function<WeatherDescriptionPojo,Boolean> filterMarchAprilMay = e-> selectedMonths.contains(e.getDateTime().getMonthOfYear());
        JavaRDD<WeatherDescriptionPojo> selectedMonthRDD = descriptionRDD.filter(filterMarchAprilMay);

        //Cancello tutti i POJO che NON contengono Sky is clear come descrizione
        JavaRDD<WeatherDescriptionPojo> clearSkyMonthRDD = selectedMonthRDD.filter(wdp -> wdp.getWeatherCondition().equals("sky is clear"));


        //Cancello tutti i POJO che sono fuori da un range orario prestabilito
        JavaRDD<WeatherDescriptionPojo> inHoursRangeRDD = clearSkyMonthRDD.filter(wdp -> wdp.getLocalDateTime().hourOfDay().compareTo(startHour) >=0 &&
                wdp.getLocalDateTime().hourOfDay().compareTo(endHour)<=0 );


        //Chiave è la Tupla4(Anno, Città,Mese, giorno del mese), value è 1
        JavaPairRDD<Tuple4<Integer,String,Integer,Integer>,Integer> keyedYearCityMonthDayRDD = inHoursRangeRDD.mapToPair(
                wdp -> new Tuple2<>(new Tuple4<>
                        (wdp.getDateTime().getYear(), wdp.getCity(), wdp.getDateTime().getMonthOfYear(), wdp.getDateTime().getDayOfMonth()),STATICONE));

        // coppia (città+giornodelmese, somma degli orari con cielo sereno) -> conto quante ore di cielo sereno ci sono state in un giorno in una città
        JavaPairRDD<Tuple4<Integer, String, Integer, Integer>, Integer> reducedYearCityMonthDayKeyedRDD = keyedYearCityMonthDayRDD.reduceByKey((a, b) -> a+b);

        // Cancello tutti gli elementi che NON hanno almeno 8 ore di cielo sereno
        //Logica di filtraggio implementabile secondo criterio a piacere
        JavaPairRDD<Tuple4<Integer, String, Integer, Integer>, Integer> filteredByClearDayCriteriumRDD = reducedYearCityMonthDayKeyedRDD.filter(wdp-> wdp._2() >= 8);

        //Quella che prima era la chiave "Anno,Città,Mese,GiornoDelMese" ora diventa una coppia chiave valore --> <K,V> = <<Anno,città,Mese>, 1>
        JavaPairRDD<Tuple3<Integer, String, Integer>, Integer> reKeyedRDD = filteredByClearDayCriteriumRDD.mapToPair(wdp -> {
            Tuple4<Integer, String, Integer, Integer> oldK = wdp._1();
            Tuple3<Integer, String, Integer> newK = new Tuple3<>(oldK._1(),oldK._2(),oldK._3());
            return new Tuple2<>(newK,STATICONE);
        });

        // riduco sulla chiave, quindi sommo le occorrenze di giorni sereni in una città
        JavaPairRDD<Tuple3<Integer, String, Integer>, Integer> reducedAlmostRDD = reKeyedRDD.reduceByKey((a,b) -> a+b);

        // Seguendo la logica di prima, la somma dei giorni deve essere maggiore di 15. Le chiavi rimaste sono le città con almeno 15 giorni di sole a Marzo.
        JavaPairRDD<Tuple3<Integer, String, Integer>, Integer> moreThanFifteenDaysRDD = reducedAlmostRDD.filter(wdp-> wdp._2() >= 15);

        //Nuovo mappaggio delle chiavi, chiave è <Anno,Città>, value è 1
        JavaPairRDD<Tuple2<Integer,String>,Integer> newReKeyed = moreThanFifteenDaysRDD.mapToPair(wdp -> {

            Tuple3<Integer, String, Integer> oldK = wdp._1();
            Tuple2<Integer,String> newK = new Tuple2<>(oldK._1(),oldK._2());

            return new Tuple2<>(newK,STATICONE);
        });

        //sommiamo i valori ottenendo il numero di mesi in cui una certà città in un certo anno è stata soleggiata...
        JavaPairRDD<Tuple2<Integer, String>, Integer> sumMonthsRDD = newReKeyed.reduceByKey((a,b) -> a+b);

        //Prendiamo i valori con value = 3, ovvero quelli che haano avuto i criteri per tutti e 3 i mesi
        JavaPairRDD<Tuple2<Integer, String>, Integer> finalRDD = sumMonthsRDD.filter(wdp-> wdp._2() == 3);

        JavaPairRDD<Integer,String> citiesPerRDD = finalRDD.mapToPair(wdp-> new Tuple2<>(wdp._1()._1(),wdp._1()._2()));

        //Final map per avere record del tipo (Anno,Lista città)
        JavaPairRDD<Integer, Iterable<String>> resultRDD = citiesPerRDD.groupByKey();*/

        //prendo tutti i POJO che si riferiscono a Marzo, Aprile e Maggio
        Function<WeatherDescriptionPojo,Boolean> filterMarchAprilMay = e-> selectedMonths.contains(e.getDateTime().getMonthOfYear());
        JavaPairRDD<Integer, Iterable<String>> resultRDD = descriptionRDD.filter(filterMarchAprilMay)
                .filter(wdp -> wdp.getWeatherCondition().equals("sky is clear"))
                .filter(wdp -> wdp.getLocalDateTime().hourOfDay().compareTo(startHour) >=0 &&
                        wdp.getLocalDateTime().hourOfDay().compareTo(endHour)<=0 )
                .mapToPair( wdp -> new Tuple2<>(new Tuple4<> (wdp.getDateTime().getYear(), wdp.getCity(), wdp.getDateTime().getMonthOfYear(), wdp.getDateTime().getDayOfMonth()),STATICONE))
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

        System.out.printf("Query 1 completed in %f seconds\n",delta);

        tk.startPhase("Collecting Query 1");
        List<Tuple2<Integer, Iterable<String>>> list = resultRDD.collect();
        tk.endPhase("Collecting Query 1");

        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Tuple2 o: list
             ) {
            count++;
            System.out.println(o._1());
            sb.append(o._1().toString()+"\n");
            System.out.println(o._2());
            sb.append(o._2().toString()+"\n");
        }

        System.out.println(tk.getTableTimes());

        //HDFSHelper.getInstance().writeStringToHDFS("/output","query1.txt",sb.toString());



    }

}

