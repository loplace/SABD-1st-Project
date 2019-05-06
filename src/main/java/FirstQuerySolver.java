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
import scala.Tuple2;

public class FirstQuerySolver {


    public static void main(String args[]){

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
        Integer[] months = {3,4,5};
        List<Integer> selectedMonths = Arrays.asList(months);

        try {
            in = new FileReader(path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord(false).parse(in);
            headers = records.iterator().next().toMap().keySet();
            headers.remove("datetime");
            headers.forEach(System.out::println);
            List<WeatherDescriptionPojo> weatherDescriptionPojos = new ArrayList<>();
            Iterator<CSVRecord> iterator = records.iterator();
            while (iterator.hasNext()){

                CSVRecord record = iterator.next();
               for (String field:headers){

                   String dateTime = record.get("datetime");
                   String description = record.get(field);


                   if (!description.isEmpty() && !dateTime.isEmpty()){

                           WeatherDescriptionPojo weatherDescriptionPojo = new WeatherDescriptionPojo(field,dateTime,description);
                           weatherDescriptionPojos.add(weatherDescriptionPojo);

                       }
               }

              // weatherDescriptionPojos.forEach(System.out::println);

            }

            Function<WeatherDescriptionPojo,Boolean> filterMarchAprilMay = e-> selectedMonths.contains(e.getDateTime().getMonthOfYear());

            long start = System.currentTimeMillis();

            JavaRDD<WeatherDescriptionPojo> descriptionRDD = jsc.parallelize(weatherDescriptionPojos);

            JavaRDD<WeatherDescriptionPojo> filteredRDD = descriptionRDD.filter(filterMarchAprilMay);


            // key is City+yyyy-MM, value is all pojos within a month representing an hour of clear sky.
/*            JavaPairRDD<String,Iterable<WeatherDescriptionPojo>> filteredKeyedByCity = keyedByCityRDD.filter(
                    wsp -> {
                        Iterator<WeatherDescriptionPojo> iterator3 = wsp._2().iterator();
                        while (iterator3.hasNext()) {
                            WeatherDescriptionPojo w = iterator3.next();
                            if (w.getWeatherCondition().equals("sky is clear")) {
                                return true;
                            }
                        }
                        return false;
                    });*/

            JavaPairRDD<String,Iterable<WeatherDescriptionPojo>> keyedByCityRDD = filteredRDD.groupBy(
                    wdp -> wdp.getCity() + "_" + wdp.getDateTime().toString("yyyy-MM-dd"));

/*            System.out.println(keyedByCityRDD.count());
            Iterator<WeatherDescriptionPojo> iterator1 = keyedByCityRDD.take(1).get(0)._2().iterator();
            int i=0;
            while (iterator1.hasNext()) {
                iterator1.next();
               i++;
            }
            System.out.println(i);*/

            // key is city+yyyy-MM-dd, value is 24 Pojos, one per hour, representing a clear day.
            JavaPairRDD<String,Iterable<WeatherDescriptionPojo>> filteredKeyedByCity = keyedByCityRDD.filter(
                    wsp -> {
                        Iterator<WeatherDescriptionPojo> iterator3 = wsp._2().iterator();
                        int count = 0;
                        while (iterator3.hasNext()) {
                            WeatherDescriptionPojo w = iterator3.next();
                            if (w.getWeatherCondition().equals("sky is clear")) {
                                count++;
                            }
                        }
                        return count>12;
                    });

            JavaPairRDD keyedByCityMonthRDD = filteredKeyedByCity.groupBy(
                    wdp-> wdp._2().iterator().next().getCity() + "_" + wdp._2().iterator().next().getDateTime().toString("yyyy-MM")
            );




            List<Tuple2<String, Iterable<WeatherDescriptionPojo>>> list = keyedByCityMonthRDD.collect();


           Iterator<WeatherDescriptionPojo> iterator2 = list.get(0)._2.iterator();
            while (iterator2.hasNext()) {
                System.out.println(iterator2.next());
            }

/*            long endTime = System.currentTimeMillis();
            long howLong = endTime-start;
            System.out.println(howLong/1000L);*/

        } catch (IOException e) {
            e.printStackTrace();
        }






    }

}
