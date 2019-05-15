package utils.hdfs;

import model.CityModel;
import model.WeatherDescriptionPojo;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaRDD;
import parser.rddloader.WeatherRDDLoaderFromParquetFile;
import parser.rddloader.WeatherRDDLoaderFromTextFile;
import parser.validators.IMeasurementValidator;
import utils.configuration.AppConfiguration;

import java.util.Map;

public class HDFSDataLoader {

    public enum DATASETNAME {
        TEMPERATURE, PRESSURE, HUMIDITY, WEATHER_DESC
    }
    private static String fileFormat = "csv";

    static Map<String, CityModel> cities;

    public static void setCityMap(Map<String, CityModel> c) {
        cities = c;
    }

    public static JavaRDD<WeatherMeasurementPojo> loadWeatherMeasurementPojo(String filePath, IMeasurementValidator validator) {

        if (fileFormat.equals("csv")) {
            return new WeatherRDDLoaderFromTextFile(cities)
                    .loadWeatherMeasurementPojoRDD(filePath,validator);
        }
        if (fileFormat.equals("parquet")) {
            return new WeatherRDDLoaderFromParquetFile(cities)
                    .loadWeatherMeasurementPojoRDD(filePath,validator);
        }
        return null;
    }

    public static JavaRDD<WeatherDescriptionPojo> loadWeatherDescriptiontPojo(String filePath) {
        if (fileFormat.equals("csv")) {
            return new WeatherRDDLoaderFromTextFile(cities)
                    .loadWeatherDescriptionPojoRDD(filePath);
        }
        if (fileFormat.equals("parquet")) {
            return new WeatherRDDLoaderFromParquetFile(cities)
                    .loadWeatherDescriptionPojoRDD(filePath);
        }
        return null;
    }

    public static void setFileFormat(String format) {
        if (format!=null || !format.isEmpty()) {
            fileFormat = format;
        }
    }

    public static String getDataSetFilePath(DATASETNAME datasetName) {
        String result = "";

        if (fileFormat.equals("csv")) {
            switch(datasetName) {
                case TEMPERATURE:
                    result = AppConfiguration.getProperty("dataset.csv.temperature");
                    break;
                case PRESSURE:
                    result = AppConfiguration.getProperty("dataset.csv.pressure");
                    break;
                case HUMIDITY:
                    result = AppConfiguration.getProperty("dataset.csv.humidity");
                    break;
                case WEATHER_DESC:
                    result = AppConfiguration.getProperty("dataset.csv.weatherdesc");
                    break;
                default:
                    result = "";
            }
        }

        if (fileFormat.equals("parquet")) {
            switch(datasetName) {
                case TEMPERATURE:
                    result = AppConfiguration.getProperty("dataset.parquet.temperature");
                    break;
                case PRESSURE:
                    result = AppConfiguration.getProperty("dataset.parquet.pressure");
                    break;
                case HUMIDITY:
                    result = AppConfiguration.getProperty("dataset.parquet.humidity");
                    break;
                case WEATHER_DESC:
                    result = AppConfiguration.getProperty("dataset.parquet.weatherdesc");
                    break;
                default:
                    result = "";
            }
        }

        return result;
    }
}
