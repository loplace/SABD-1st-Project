package queries;

import java.io.*;
import java.util.*;

import model.CityModel;
import model.WeatherDescriptionPojo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.joda.time.LocalTime;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.configuration.AppConfiguration;
import utils.hdfs.HDFSHelper;
import utils.locationinfo.CityAttributesPreprocessor;

public class FirstQuerySolver {

    public final static int STATICONE = 1;


    public static void main(String args[]) throws IOException {



        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("First query Solver");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");

/*        LocalTime earlier = new LocalTime("23:00:00");
        LocalTime later = new LocalTime("23:12:34");

        System.out.println(earlier.compareTo(later));   // -1
        System.out.println(later.compareTo(earlier));   // 1
        System.out.println(earlier.compareTo(earlier)); // 0*/

        LocalTime startHour = new LocalTime("08:00:00");
        LocalTime endHour = new LocalTime("18:00:00");


        // Load and parse data
        //String path = args[0];
        //String path = "/home/federico/Scaricati/prj1_dataset/weather_description.csv";
        //String path = "/Users/antonio/Downloads/prj1_dataset/weather_description.csv";
        String stringPath = AppConfiguration.getProperty("dataset.csv.weatherdesc");

        Iterable<CSVRecord> records;
        Reader in = null;
        Set<String> headers = null;
        Integer[] months = {3,4,5};
        List<Integer> selectedMonths = Arrays.asList(months);

        InputStream wrappedStream=null;
        try {
            //in = new FileReader(stringPath);
            if (stringPath.startsWith("hdfs://")) {
                Path hdfsreadpath = new Path(stringPath);
                wrappedStream = HDFSHelper.getInstance().getFs().open(hdfsreadpath).getWrappedStream();
                in = new InputStreamReader(wrappedStream);
            }


        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }



        //new, use preprocessor to grab city ID for correct UTC
        CityAttributesPreprocessor cityAttributesPreprocessor = new CityAttributesPreprocessor();
        Map<String, CityModel> cities = cityAttributesPreprocessor.process().getCities();

        records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord(false).parse(in);
        headers = records.iterator().next().toMap().keySet();
        headers.remove("datetime");
//        headers.forEach(System.out::println);
        List<WeatherDescriptionPojo> weatherDescriptionPojos = new ArrayList<>();
        Iterator<CSVRecord> iterator = records.iterator();
        while (iterator.hasNext()) {

            CSVRecord record = iterator.next();
            for (String field : headers) {

                String dateTime = record.get("datetime");
                String description = record.get(field);


                if (!description.isEmpty() && !dateTime.isEmpty()) {

                    WeatherDescriptionPojo weatherDescriptionPojo = new WeatherDescriptionPojo(field, dateTime, description);

                    String key = weatherDescriptionPojo.getCity();
                    CityModel cityModel = cities.get(key);
                    if (cityModel != null){
                        String citytimezone = cityModel.getTimezone();
                        weatherDescriptionPojo.setDateTimezone(citytimezone);
                    }

                    weatherDescriptionPojos.add(weatherDescriptionPojo);

                }
            }

        }

        final double start = System.nanoTime();

        //TODO PARALLELIZE BRUTTA E CATTIVA, TEXTFILE BUONA!
        JavaRDD<WeatherDescriptionPojo> descriptionRDD = jsc.parallelize(weatherDescriptionPojos,850);

        descriptionRDD.foreach(wdp -> {
            wdp.setDateTimezone(cities.get(wdp.getCity()).getTimezone());
        });
     //   System.out.println("descriptionRDD.count(): " + descriptionRDD.count());


        //prendo tutti i POJO che si riferiscono a Marzo, Aprile e Maggio
        Function<WeatherDescriptionPojo,Boolean> filterMarchAprilMay = e-> selectedMonths.contains(e.getDateTime().getMonthOfYear());
        JavaRDD<WeatherDescriptionPojo> selectedMonthRDD = descriptionRDD.filter(filterMarchAprilMay);

        //Cancello tutti i POJO che NON contengono Sky is clear come descrizione
        JavaRDD<WeatherDescriptionPojo> clearSkyMonthRDD = selectedMonthRDD.filter(wdp -> wdp.getWeatherCondition().equals("sky is clear"));

     //   System.out.println("clearSkyMonthRDD.count(): " + clearSkyMonthRDD.count());

        //Cancello tutti i POJO che sono fuori da un range orario prestabilito
        JavaRDD<WeatherDescriptionPojo> inHoursRangeRDD = clearSkyMonthRDD.filter(wdp -> wdp.getLocalDateTime().hourOfDay().compareTo(startHour) >=0 &&
                wdp.getLocalDateTime().hourOfDay().compareTo(endHour)<=0 );

    //    System.out.println("inHoursRangeRDD.count(): " + inHoursRangeRDD.count());

        /*List<WeatherDescriptionPojo> firstTen = clearSkyMonthRDD.take(10);
        firstTen.forEach(System.out::println);*/

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
        JavaPairRDD<Integer, Iterable<String>> resultRDD = citiesPerRDD.groupByKey();

        final double end = System.nanoTime();

        final double delta = (end - start)/1000000000L;

        System.out.printf("Query 1 completed in %f seconds\n",delta);



        List<Tuple2<Integer, Iterable<String>>> list = resultRDD.collect();

        int count = 0;
        for (Tuple2 o: list
             ) {
            count++;
            System.out.println(o._1());
            System.out.println(o._2());
        }


    }

}

