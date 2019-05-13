package parser;

import model.CityModel;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import utils.configuration.AppConfiguration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class CityAttributeParser {

    public final static String csvpath = AppConfiguration.getProperty("dataset.csv.cityattributes");


    private Map<String, CityModel> cities = null;

    public CityAttributeParser() {
        this.cities = new HashMap<>();
    }

    public void parse () throws IOException {


        Iterable<CSVRecord> records;
        Reader in = null;

        try {
            in = new FileReader(csvpath);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }

        records = CSVFormat.DEFAULT.withHeader().parse(in);


        Iterator<CSVRecord> iterator = records.iterator();
        while (iterator.hasNext()) {

            CSVRecord record = iterator.next();
            String cityName = record.get("City");
            String latitude = record.get("Latitude");
            String longitude = record.get("Longitude");

            CityModel newCity = new CityModel(cityName,Double.parseDouble(latitude),Double.parseDouble(longitude));
            this.cities.put(cityName,newCity);

        }


    }

    public void printCities(){

        for(Map.Entry<String, CityModel> entry:this.cities.entrySet()){

            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }

    public Map<String, CityModel> getCities() {

        return cities;
    }

}