package parser;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
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
    }

    public JavaRDD<WeatherMeasurementPojo> loadWeatherMeasurementPojoRDD(String parquetFilePath) {

        Dataset<Row> df = sqlContext.read().parquet(parquetFilePath);
        JavaRDD<Row> rowJavaRDD = df.javaRDD();
        List<Row> collect = rowJavaRDD.collect();
        collect.forEach(System.out::println);

        /*JavaRDD<String> csvData = jsc.textFile(filePath);
        String csvHeader = csvData.first();
        JavaRDD<String> nonHeaderCSV = csvData.filter(row -> !row.equals(csvHeader));

        JavaRDD<WeatherMeasurementPojo> result = nonHeaderCSV.flatMap(
                new WeatherMeasurementParserFlatMap(csvHeader).setCitiesMap(cities)
        );*/



        return null;
    }

    public static void main(String[] args) {

        WeatherRDDLoaderFromParquetFile w = new WeatherRDDLoaderFromParquetFile(null);
        w.loadWeatherMeasurementPojoRDD("/home/federico/Scaricati/humidity.prq");

    }
}
