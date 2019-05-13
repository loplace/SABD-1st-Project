package parser.old;

import model.CityModel;
import model.WeatherMeasurementPojo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class WeatherMeasurementCSVParser {


    public static List<WeatherMeasurementPojo> csvToMeasurementPojo(String csvPath, Map<String, CityModel> cities) {

        Reader in = null;
        try {
            in = new FileReader(csvPath);
        } catch (FileNotFoundException e) {
            System.err.println("FileNotFoundException: "+e.getMessage());
        }

        List<WeatherMeasurementPojo> measurements = new ArrayList<>();
        Iterable<CSVRecord> records = null;
        Set<String> headers;

        try {
            records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord(false).parse(in);
        } catch (IOException e) {
            System.err.println("IOException: "+e.getMessage());
        }
        headers = records.iterator().next().toMap().keySet();
        headers.remove("datetime");

        Iterator<CSVRecord> iterator = records.iterator();
        while (iterator.hasNext()) {

            CSVRecord record = iterator.next();
            for (String field : headers) {

                String dateTime = record.get("datetime");
                String measurementValue = record.get(field);

                if (!measurementValue.isEmpty() && !dateTime.isEmpty()) {
                    double measurement = Double.parseDouble(measurementValue);
                    WeatherMeasurementPojo wmp = new WeatherMeasurementPojo(field, dateTime, measurement);

                    String key = wmp.getCity();
                    CityModel cityModel = cities.get(key);
                    if (cityModel != null){
                        String country = cityModel.getCountry();
                        wmp.setCountry(country);
                    }
                    measurements.add(wmp);
                }
            }
        }
        return  measurements;
    }
}
