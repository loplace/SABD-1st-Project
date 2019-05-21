package parser;

import model.CityModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.configuration.AppConfiguration;
import utils.hdfs.HDFSHelper;
import utils.spark.SparkContextSingleton;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

public class CityAttributeParser {

    public final static String csvpath = AppConfiguration.getProperty("dataset.csv.cityattributes_processed");


    private Map<String, CityModel> cities = null;
    private JavaSparkContext jsc;

    public CityAttributeParser() {
        jsc = SparkContextSingleton.getInstance().getContext();
        this.cities = new HashMap<>();
    }

    public void parse() {
        //parseWithRDD();
        parseSimpleText();
    }

    private void parseWithRDD() {
        JavaRDD<String> csvData = jsc.textFile(csvpath);
        String csvHeader = csvData.first();

        JavaRDD<String> nonHeaderCSV = csvData.filter(row -> !row.equals(csvHeader));

        JavaRDD<CityModel> cityModelRDD = nonHeaderCSV.map(line -> CityAttributeParser.parseLine(line));

        Iterator<CityModel> cityModelIterator = cityModelRDD.collect().iterator();
        while (cityModelIterator.hasNext()) {
            CityModel m = cityModelIterator.next();
            cities.put(m.getCity(),m);
        }
    }

    private void parseSimpleText() {
        String lines = HDFSHelper.getInstance().readStringFromHDFS(csvpath);
        Scanner scanner = new Scanner(lines);

        //pop header
        scanner.nextLine();

        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            CityModel m = CityAttributeParser.parseLine(line);
            cities.put(m.getCity(),m);
        }
    }


    public static CityModel parseLine(String line) {
        String[] tokens = line.split(",");
        String cityName = tokens[0];
        String strLatitude = tokens[1];
        String strLongitude = tokens[2];

        String strTimeZone = tokens[3];
        String strCountry = tokens[4];


        double latitude = Double.parseDouble(strLatitude);
        double longitude = Double.parseDouble(strLongitude);

        CityModel model = new CityModel(cityName,latitude,longitude);
        model.setTimezone(strTimeZone);
        model.setCountry(strCountry);

        return model;
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
