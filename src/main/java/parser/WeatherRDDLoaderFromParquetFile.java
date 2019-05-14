package parser;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.collection.Seq;
import utils.configuration.AppConfiguration;
import utils.spark.SparkContextSingleton;

import java.util.Arrays;
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

    public JavaRDD<WeatherMeasurementPojo> loadWeatherMeasurementPojoRDD(String parquetFilePath) {

        Dataset<Row> df = sqlContext.read().parquet(parquetFilePath);
        JavaRDD<Row> rowJavaRDD = df.toJavaRDD();

        JavaRDD<WeatherMeasurementPojo> result = rowJavaRDD.flatMap(
                row -> WeatherMeasurementParser.parseParquetRow(row)
        );

        return result;
    }
}
