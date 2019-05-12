package parser;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.Map;

public class WeatherMeasurementParserFlatMap implements FlatMapFunction<String, WeatherMeasurementPojo> {

    private String header;
    private Map<String, CityModel> citiesMap;

    public WeatherMeasurementParserFlatMap(String csvHeader) {
        header = csvHeader;
    }

    public WeatherMeasurementParserFlatMap setCitiesMap(Map<String, CityModel> cities) {
        citiesMap = cities;
        return this;
    }

    @Override
    public Iterator<WeatherMeasurementPojo> call(String line) {
        WeatherMeasurementParser.setCitiesMap(citiesMap);

        return WeatherMeasurementParser.parseLine(header,line);
    }
}
