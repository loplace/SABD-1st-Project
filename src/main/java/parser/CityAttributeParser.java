package parser;

import model.CityModel;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.configuration.AppConfiguration;
import utils.hdfs.HDFSHelper;
import utils.spark.SparkContextSingleton;

import java.io.*;
import java.util.*;

public class CityAttributeParser {

    public final static String csvpath = AppConfiguration.getProperty("dataset.csv.cityattributes");


    private Map<String, CityModel> cities = null;
    private JavaSparkContext jsc;

    public CityAttributeParser() {
        jsc = SparkContextSingleton.getInstance("local").getContext();
        this.cities = new HashMap<>();
    }

    public void parse() {
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

    public static CityModel parseLine(String line) {
        String[] tokens = line.split(",");
        String cityName = tokens[0];
        String strLatitude = tokens[1];
        String strLongitude = tokens[2];

        double latitude = Double.parseDouble(strLatitude);
        double longitude = Double.parseDouble(strLongitude);

        CityModel model = new CityModel(cityName,latitude,longitude);
        return model;
    }

    /*public void parse () throws IOException {


        Iterable<CSVRecord> records;
        Reader in = null;
        InputStream wrappedStream=null;
        try {
            in = new FileReader(csvpath);
            if (csvpath.startsWith("hdfs://")) {
                Path hdfsreadpath = new Path(csvpath);
                wrappedStream = HDFSHelper.getInstance().getFs().open(hdfsreadpath).getWrappedStream();
                in = new InputStreamReader(wrappedStream);
            }
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


    }*/

    public void printCities(){

        for(Map.Entry<String, CityModel> entry:this.cities.entrySet()){

            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }

    public Map<String, CityModel> getCities() {

        return cities;
    }

}
