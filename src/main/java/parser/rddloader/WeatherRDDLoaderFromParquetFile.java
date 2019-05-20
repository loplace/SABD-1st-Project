package parser.rddloader;

import model.CityModel;
import model.WeatherDescriptionPojo;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import parser.description.WeatherDescriptionParser;
import parser.measurement.WeatherMeasurementParser;
import parser.validators.IMeasurementValidator;
import utils.spark.SparkContextSingleton;

import java.util.List;
import java.util.Map;

public class WeatherRDDLoaderFromParquetFile {


    private JavaSparkContext jsc;
    private Map<String, CityModel> cities;
    private SQLContext sqlContext;

    public WeatherRDDLoaderFromParquetFile(Map<String, CityModel> citiesMap) {
        jsc = SparkContextSingleton.getInstance().getContext();
        sqlContext = new SQLContext(jsc);
        cities = citiesMap;
        WeatherMeasurementParser.setCitiesMap(cities);
    }

    public JavaRDD<WeatherMeasurementPojo> loadWeatherMeasurementPojoRDD(String parquetFilePath, IMeasurementValidator validator) {

        Dataset<Row> df = sqlContext.read().parquet(parquetFilePath);
        JavaRDD<Row> rowJavaRDD = df.toJavaRDD();

        JavaRDD<WeatherMeasurementPojo> result = rowJavaRDD.flatMap(
                row -> WeatherMeasurementParser.parseParquetRow(row,validator)
        );

        return result;
    }

    public JavaRDD<WeatherDescriptionPojo> loadWeatherDescriptionPojoRDD(String parquetFilePath) {

        Dataset<Row> df = sqlContext.read().parquet(parquetFilePath);
        JavaRDD<Row> rowJavaRDD = df.toJavaRDD();

        JavaRDD<WeatherDescriptionPojo> result = rowJavaRDD.flatMap(
                row -> WeatherDescriptionParser.parseParquetRow(row)
        );
        return result;
    }
}
