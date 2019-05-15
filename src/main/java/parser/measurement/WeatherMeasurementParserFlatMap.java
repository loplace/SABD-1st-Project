package parser.measurement;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.api.java.function.FlatMapFunction;
import parser.validators.IMeasurementValidator;

import java.util.Iterator;
import java.util.Map;

public class WeatherMeasurementParserFlatMap implements FlatMapFunction<String, WeatherMeasurementPojo> {

    private String header;
    private Map<String, CityModel> citiesMap;
    private IMeasurementValidator validator;

    public WeatherMeasurementParserFlatMap(String csvHeader) {
        header = csvHeader;
    }

    public WeatherMeasurementParserFlatMap setCitiesMap(Map<String, CityModel> cities) {
        citiesMap = cities;
        WeatherMeasurementParser.setCitiesMap(citiesMap);
        return this;
    }

    public WeatherMeasurementParserFlatMap setValidator(IMeasurementValidator v) {
        //WeatherMeasurementParser.setValidator(validator);
        validator = v;
        return this;
    }

    @Override
    public Iterator<WeatherMeasurementPojo> call(String line) {
        return WeatherMeasurementParser.parseLine(header,line,validator);
    }
}
