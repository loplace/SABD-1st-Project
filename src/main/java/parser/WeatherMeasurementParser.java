package parser;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.spark.sql.Row;
import scala.xml.parsing.FatalError;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WeatherMeasurementParser {

    private static Map<String, CityModel> citiesMap;

    public static void setCitiesMap(Map<String, CityModel> c) {
        citiesMap = c;
    }

    public static Iterator<WeatherMeasurementPojo> parseLine(String completeHeader, String line) {

        List<WeatherMeasurementPojo> result = new ArrayList<>();
        String[] headers = completeHeader.split(","); // array di 34 elementi (datetime + 33 città)
        String[] tokens = line.split(","); // array di 34 elementi (datetime + 33 città)

        int noHeaderItems = headers.length;
        int noTokensItems = tokens.length;

        if (noHeaderItems == noTokensItems) {

            for (int i = 1; i < noTokensItems; i++) {

                String dateTime = tokens[0];
                String cityName = headers[i];

                String rawValue = tokens[i];

                if (!rawValue.isEmpty() && !dateTime.isEmpty()) {

                    double value = Double.parseDouble(rawValue); // to catch exceptions
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


    public static Iterator<WeatherMeasurementPojo> parseParquetRow(Row row) {

        List<WeatherMeasurementPojo> result = new ArrayList<>();

        String[] headers = row.schema().fieldNames();
        int noHeaderItems = headers.length;

        for (int i=1; i<noHeaderItems; i++) {

            String dateTime = row.getString(0);
            String cityName = headers[i];

            String rawValue;
            Object obj = row.getAs(cityName);
            if (obj!=null) {

                try {
                    rawValue = row.getAs(cityName);
                } catch (ClassCastException e) {
                    rawValue = "";
                }

                if (rawValue!= null && !rawValue.isEmpty() && !dateTime.isEmpty()) {

                    double value = Double.parseDouble(rawValue); // to catch exceptions
                    WeatherMeasurementPojo wmp = new WeatherMeasurementPojo(cityName, dateTime, value);

                    CityModel keyModel = citiesMap.get(cityName);
                    if (citiesMap!=null && keyModel!=null) {
                        wmp.setCountry(keyModel.getCountry());
                    }
                    result.add(wmp);
                }
            }
        }

        return result.iterator();
    }
}
