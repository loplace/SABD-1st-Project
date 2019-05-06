package utils;

import java.io.*;
import java.util.*;

import POJO.WeatherDescriptionPojo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class NewFirstQuerySolver {


    public static void main(String args[]) throws IOException {

        System.out.println("First query solver");

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("First query Solver");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Load and parse data
        //String path = args[0];
        String path = "/home/federico/Scaricati/prj1_dataset/weather_description.csv";

        Iterable<CSVRecord> records;
        Reader in = null;
        Set<String> headers = null;
        int march = 3;
        int april = 4;
        int may = 5;


        try {
            in = new FileReader(path);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }

        records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord(false).parse(in);
        headers = records.iterator().next().toMap().keySet();
        headers.remove("datetime");
        headers.forEach(System.out::println);
        List<WeatherDescriptionPojo> weatherDescriptionPojos = new ArrayList<>();
        Iterator<CSVRecord> iterator = records.iterator();
        while (iterator.hasNext()) {

            CSVRecord record = iterator.next();
            for (String field : headers) {

                String dateTime = record.get("datetime");
                String description = record.get(field);


                if (!description.isEmpty() && !dateTime.isEmpty()) {

                    WeatherDescriptionPojo weatherDescriptionPojo = new WeatherDescriptionPojo(field, dateTime, description);
                    weatherDescriptionPojos.add(weatherDescriptionPojo);

                }
            }

            // weatherDescriptionPojos.forEach(System.out::println);

        }



        JavaRDD<WeatherDescriptionPojo> descriptionRDD = jsc.parallelize(weatherDescriptionPojos).cache();
        JavaRDD<WeatherDescriptionPojo> primoAnnoRDD = descriptionRDD.filter(wdp -> wdp.getDateTime().getYear() == 2012);
        JavaRDD<WeatherDescriptionPojo> secondoAnnoRDD = descriptionRDD.filter(wdp -> wdp.getDateTime().getYear() == 2013);
        JavaRDD<WeatherDescriptionPojo> terzoAnnoRDD = descriptionRDD.filter(wdp -> wdp.getDateTime().getYear() == 2014);
        JavaRDD<WeatherDescriptionPojo> quartoAnnoRDD = descriptionRDD.filter(wdp -> wdp.getDateTime().getYear() == 2015);
        JavaRDD<WeatherDescriptionPojo> quintoAnnoRDD = descriptionRDD.filter(wdp -> wdp.getDateTime().getYear() == 2016);
        JavaRDD<WeatherDescriptionPojo> sestoAnnoRDD = descriptionRDD.filter(wdp -> wdp.getDateTime().getYear() == 2017);

        //prendo tutti i POJO che si riferiscono a Marzo
        JavaRDD<WeatherDescriptionPojo> primoAnnoMarchRDD = primoAnnoRDD.filter(wdp -> wdp.getDateTime().getMonthOfYear() == 12);

        //Cancello tutti i POJO che NON contengono Sky is clear come descrizione
        JavaRDD<WeatherDescriptionPojo> primoAnnoMarchClearSkyRDD = primoAnnoMarchRDD.filter(wdp -> wdp.getWeatherCondition().equals("sky is clear"));

        //Chiave è la Tupla2(Città,giorno del mese), value è 1
        JavaPairRDD<Tuple2<String,Integer>,Integer> keyedRDD = primoAnnoMarchClearSkyRDD.mapToPair(
                wdp -> new Tuple2<>(new Tuple2<String,Integer>(wdp.getCity(),wdp.getDateTime().getDayOfMonth()),1));

        // coppia (città+giornodelmese, somma degli orari con cielo sereno) -> conto quante ore di cielo sereno ci sono state in un giorno in una città
        JavaPairRDD<Tuple2<String,Integer>,Integer> reducedKeyedRDD = keyedRDD.reduceByKey((a,b) -> a+b);

        // Cancello tutti gli elementi che NON hanno almeno 12 ore di cielo sereno
        JavaPairRDD<Tuple2<String,Integer>,Integer> filteredRDD = reducedKeyedRDD.filter(wdp-> wdp._2() >= 12);

        //Quella che prima era la chiave "Città,GiornoDelMese" ora diventa una coppia chiave valore --> <K,V> = città, 1
        JavaPairRDD<String,Integer> almostRDD = filteredRDD.mapToPair(wdp -> new Tuple2<>(wdp._1()._1(),1));

        // riduco sulla chiave, quindi sommo le occorrenze di giorni sereni i una città
        JavaPairRDD<String,Integer> reducedAlmostRDD = almostRDD.reduceByKey((a,b) -> a+b);

        // Seguendo la logica di prima, la somma dei giorni deve essere maggiore di 15. Le chiavi rimaste sono le città con almeno 15 giorni di sole a Marzo.
        JavaPairRDD<String,Integer> finalMarchRDD = reducedAlmostRDD.filter(wdp-> wdp._2() >= 15);

        List<Tuple2<String,Integer>> list = finalMarchRDD.collect();

        int count = 0;
        for (Tuple2 o: list
             ) {
            count++;
            System.out.println(o._1());
        }

        System.out.println(count);


    }

}

