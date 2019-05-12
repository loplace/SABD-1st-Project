package parser;

import model.CityModel;
import model.WeatherDescriptionPojo;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.function.FlatMapFunction;

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
        return this;
    }

    @Override
    public Iterator<WeatherDescriptionPojo> call(String line) {
        WeatherMeasurementParser.setCitiesMap(citiesMap);

        return WeatherDescriptionParser.parseLine(header,line);
    }
}
