package parser;

import model.CityModel;
import model.WeatherDescriptionPojo;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.spark.SparkContextSingleton;

import java.util.Map;

public class WeatherRDDLoaderFromTextFile {


    private JavaSparkContext jsc;
    private Map<String, CityModel> cities;

    public WeatherRDDLoaderFromTextFile(Map<String, CityModel> citiesMap) {
        jsc = SparkContextSingleton.getInstance().getContext();
        cities = citiesMap;
    }

    public JavaRDD<WeatherMeasurementPojo> loadWeatherMeasurementPojoRDD(String filePath) {

        JavaRDD<String> csvData = jsc.textFile(filePath);
        String csvHeader = csvData.first();
        JavaRDD<String> nonHeaderCSV = csvData.filter(row -> !row.equals(csvHeader));

        JavaRDD<WeatherMeasurementPojo> result = nonHeaderCSV.flatMap(
                new WeatherMeasurementParserFlatMap(csvHeader).setCitiesMap(cities)
        );

        return result;
    }

    public JavaRDD<WeatherDescriptionPojo> loadWeatherDescriptionPojoRDD(String filePath) {

        JavaRDD<String> csvData = jsc.textFile(filePath);
        String csvHeader = csvData.first();
        JavaRDD<String> nonHeaderCSV = csvData.filter(row -> !row.equals(csvHeader));

        JavaRDD<WeatherDescriptionPojo> result = nonHeaderCSV.flatMap(
                new WeatherDescriptionParserFlatMap(csvHeader).setCitiesMap(cities)
        );

        return result;
    }
}
