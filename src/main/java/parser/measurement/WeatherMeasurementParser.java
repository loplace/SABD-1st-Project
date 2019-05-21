package parser.measurement;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.sql.Row;
import parser.validators.IMeasurementValidator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WeatherMeasurementParser {

    private static Map<String, CityModel> citiesMap;


    public static void setCitiesMap(Map<String, CityModel> c) {
        citiesMap = c;
    }



    public static Iterator<WeatherMeasurementPojo> parseLine(String completeHeader, String line, IMeasurementValidator validator) {

        if (validator == null) {
            throw new IllegalStateException("Validator must be set");
        }

        List<WeatherMeasurementPojo> result = new ArrayList<>();
        String[] headers = completeHeader.split(","); // array di 34 elementi (datetime + 33 città)
        String[] tokens = line.split(",",-1); // array di 34 elementi (datetime + 33 città)

        int noHeaderItems = headers.length;
        int noTokensItems = tokens.length;

        if (noHeaderItems == noTokensItems) {

            for (int i = 1; i < noTokensItems; i++) {

                String dateTime = tokens[0];
                String cityName = headers[i];

                String rawValue = tokens[i];

                if (validator.isValid(rawValue)) {
                    double value = validator.parseValue(rawValue);
                    WeatherMeasurementPojo wmp = new WeatherMeasurementPojo(cityName, dateTime, value);

                    if (citiesMap!=null) {
                        wmp.setCountry(citiesMap.get(cityName).getCountry());
                    }
                    result.add(wmp);
                }
            }
        }
        return result.iterator();
    }

    public static Iterator<WeatherMeasurementPojo> parseParquetRow(Row row,IMeasurementValidator validator) {

        List<WeatherMeasurementPojo> result = new ArrayList<>();

        String[] headers = row.schema().fieldNames();
        int noHeaderItems = headers.length;

        for (int i=1; i<noHeaderItems; i++) {

            String dateTime = row.getString(0);
            String cityName = headers[i];

            String rawValue;
            try {
                if (row.getAs(cityName)!=null) {
                    rawValue = (String) row.getAs(cityName);

                    if (validator.isValid(rawValue)) {
                        double value = validator.parseValue(rawValue);
                        WeatherMeasurementPojo wmp = new WeatherMeasurementPojo(cityName, dateTime, value);

                        if (citiesMap!=null) {
                            wmp.setCountry(citiesMap.get(cityName).getCountry());
                        }
                        result.add(wmp);
                    }
                }

            } catch (NullPointerException e) {
                //System.err.println(cityName+" has generated a NullPointerException");
            } catch (ClassCastException e) {
                //System.err.println(cityName+" has generated a ClassCastException");
            }
        }

        return result.iterator();
    }
}
