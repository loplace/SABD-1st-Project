package utils.hdfs;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaRDD;
import parser.WeatherRDDLoaderFromParquetFile;
import parser.WeatherRDDLoaderFromTextFile;

import java.util.Map;

public class HDFSDataLoader {


    static Map<String, CityModel> cities;

    public static void setCityMap(Map<String, CityModel> c) {
        cities = c;
    }

    public static JavaRDD<WeatherMeasurementPojo> loadWeatherMeasurementPojo(String format, String filePath) {

        if (format.equals("csv")) {
            return new WeatherRDDLoaderFromTextFile(cities)
                    .loadWeatherMeasurementPojoRDD(filePath);
        }
        if (format.equals("parquet")) {
            return new WeatherRDDLoaderFromParquetFile(cities)
                    .loadWeatherMeasurementPojoRDD(filePath);
        }
        return null;
    }
}
