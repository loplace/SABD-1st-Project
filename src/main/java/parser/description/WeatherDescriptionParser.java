package parser.description;

import model.CityModel;
import model.WeatherDescriptionPojo;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WeatherDescriptionParser {

        private static Map<String, CityModel> citiesMap;

        public static void setCitiesMap(Map<String, CityModel> c) {
            citiesMap = c;
        }

        public static Iterator<WeatherDescriptionPojo> parseLine(String completeHeader, String line) {

            List<WeatherDescriptionPojo> result = new ArrayList<>();
            String[] headers = completeHeader.split(","); // array di 34 elementi (datetime + 33 città)
            String[] tokens = line.split(","); // array di 34 elementi (datetime + 33 città)

            int noHeaderItems = headers.length;
            int noTokensItems = tokens.length;

            if (noHeaderItems == noTokensItems) {
                for (int i = 1; i < noTokensItems; i++) {
                    String dateTime = tokens[0];
                    String cityName = headers[i];
                    String description = tokens[i];

                    if (!description.isEmpty() && !dateTime.isEmpty()) {
                        WeatherDescriptionPojo wdp = new WeatherDescriptionPojo(cityName, dateTime, description);

                        result.add(wdp);
                    }
                }
            }
            return result.iterator();
        }


    public static Iterator<WeatherDescriptionPojo> parseParquetRow(Row row) {

        List<WeatherDescriptionPojo> result = new ArrayList<>();

        String[] headers = row.schema().fieldNames();
        int noHeaderItems = headers.length;

        String lastError;

        for (int i=1; i<noHeaderItems; i++) {

            String dateTime = row.getString(0);
            String cityName = headers[i];

            String stringDescription;
            try {
                Object obj = row.getAs(cityName);

                if(obj!=null) {
                    stringDescription = (String) obj;
                    if (stringDescription!= null && !stringDescription.isEmpty() && !dateTime.isEmpty()) {
                        WeatherDescriptionPojo wdp = new WeatherDescriptionPojo(cityName, dateTime, stringDescription);

                        CityModel keyModel = citiesMap.get(cityName);
                        if (citiesMap!=null && keyModel!=null) {
                            wdp.setDateTimezone(keyModel.getTimezone());
                        }
                        result.add(wdp);
                    }
                }
            }catch (NullPointerException e) {
                lastError = e.getMessage();
            }
            catch (ClassCastException e) {
                lastError = e.getMessage();
            }
        }


        return result.iterator();
    }
}
