package utils;

import POJO.CityPojo;

import java.util.Map;

public class LocationInfoAssigner {


    public static void locationInfoAssign(Map<String, CityPojo> cityPojoMap) {

        for(Map.Entry<String,CityPojo> entry:cityPojoMap.entrySet()){

            CityPojo currentCity = entry.getValue();
            String locationInfo = CityLocationRetriever.retrieveLocationInfo(currentCity.getLat(),currentCity.getLon());
            String[] tokens = locationInfo.split(";");
            String timezone = tokens[0];
            String country = tokens[1];
            currentCity.setTimezone(timezone);
            currentCity.setCountry(country);
        }


    }
}
