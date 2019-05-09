import POJO.CityPojo;
import POJO.WeatherDescriptionPojo;
import POJO.WeatherMeasurementPojo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.LocalTime;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class SecondQuerySolver {


    public static void main(String[] args) throws IOException {


        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("First query Solver");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");


        // Load and parse data
        //String path = args[0];
        String pathTemperature = "/home/federico/Scaricati/prj1_dataset/temperature.csv";
        String pathPressure = "/home/federico/Scaricati/prj1_dataset/pressure.csv";
        String pathHumidity = "/home/federico/Scaricati/prj1_dataset/humidity.csv";

        // String path = "/Users/antonio/Downloads/prj1_dataset/weather_description.csv";
        // String path = "/Users/antonio/Downloads/prj1_dataset/weather_description.csv";
        // String path = "/Users/antonio/Downloads/prj1_dataset/weather_description.csv";

        Iterable<CSVRecord> records;
        Reader temperatureReader = null;
        Reader pressionReader = null;
        Reader humidityReader = null;
        Set<String> headers = null;

        try {
            temperatureReader = new FileReader(pathTemperature);
            pressionReader = new FileReader(pathPressure);
            humidityReader = new FileReader(pathHumidity);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }

        //new, use preprocessor to grab city ID for correct UTC
        CityAttributesPreprocessor cityAttributesPreprocessor = new CityAttributesPreprocessor();
        Map<String, CityPojo> cities = cityAttributesPreprocessor.process().getCities();

        List<WeatherMeasurementPojo> humidities = null;
        List<WeatherMeasurementPojo> pressures = null;
        List<WeatherMeasurementPojo> temperatures = null;
        humidities = csvToMeasurementPojo(humidityReader,cities);
        temperatures = csvToMeasurementPojo(temperatureReader,cities);
        pressures = csvToMeasurementPojo(pressionReader,cities);
    }

    private static List<WeatherMeasurementPojo> csvToMeasurementPojo(Reader in, Map<String, CityPojo> cities) throws IOException {
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
                    CityPojo cityPojo = cities.get(key);
                    if (cityPojo != null){
                        String country = cityPojo.getCountry();
                        wmp.setCountry(country);
                    }

                    measurements.add(wmp);

                }
            }

        }
        return  measurements;
    }


}
