package utils;


public class CSVtoCity {

    public static CityModel mapCSV(String line) {

        CityModel citymodel = null;
        String[] csvValues = line.split(",");

        if (csvValues.length != 3)
            return null;


        citymodel = new CityModel(
                csvValues[0], // city
                csvValues[1], // lat
                csvValues[2]    //long
        );

        return citymodel;
    }

    }
