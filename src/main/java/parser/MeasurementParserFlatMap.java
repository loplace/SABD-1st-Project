package parser;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.Map;

public class MeasurementParserFlatMap implements FlatMapFunction<String, WeatherMeasurementPojo> {

    private String header;
    private Map<String, CityModel> citiesMap;

    public MeasurementParserFlatMap(String csvHeader) {
        header = csvHeader;
    }

    public MeasurementParserFlatMap setCitiesMap(Map<String, CityModel> cities) {
        citiesMap = cities;
        return this;
    }

    @Override
    public Iterator<WeatherMeasurementPojo> call(String line) {
        MeasurementParser.setCitiesMap(citiesMap);

        return MeasurementParser.parseLine(header,line);
    }
}
