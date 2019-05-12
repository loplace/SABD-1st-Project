package parser;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.spark.SparkContextSingleton;

import java.util.Map;

public class WeatherRDDLoader {


    private JavaSparkContext jsc;
    private Map<String, CityModel> cities;

    public WeatherRDDLoader(Map<String, CityModel> citiesMap) {
        jsc = SparkContextSingleton.getInstance("local").getContext();
        cities = citiesMap;
    }

    public JavaRDD<WeatherMeasurementPojo> loadWeatherMeasurementPojoRDDFromFile(String filePath) {

        JavaRDD<String> csvData = jsc.textFile(filePath);
        String csvHeader = csvData.first();
        JavaRDD<String> nonHeaderCSV = csvData.filter(row -> !row.equals(csvHeader));

        JavaRDD<WeatherMeasurementPojo> result = nonHeaderCSV.flatMap(
                new WeatherMeasurementParserFlatMap(csvHeader).setCitiesMap(cities)
        );

        return result;
    }
}
