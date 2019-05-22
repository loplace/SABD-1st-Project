package parser.description;

import model.CityModel;
import model.WeatherDescriptionPojo;
import org.apache.spark.api.java.function.FlatMapFunction;
import parser.measurement.WeatherMeasurementParser;

import java.util.Iterator;
import java.util.Map;

public class WeatherDescriptionParserFlatMap implements FlatMapFunction<String, WeatherDescriptionPojo> {

    private String header;
    private Map<String, CityModel> citiesMap;

    public WeatherDescriptionParserFlatMap(String csvHeader) {
        header = csvHeader;
    }

    public WeatherDescriptionParserFlatMap setCitiesMap(Map<String, CityModel> cities) {
        citiesMap = cities;
        WeatherDescriptionParser.setCitiesMap(citiesMap);
        return this;
    }

    @Override
    public Iterator<WeatherDescriptionPojo> call(String line) {

        return WeatherDescriptionParser.parseLine(header,line,citiesMap);
    }
}
