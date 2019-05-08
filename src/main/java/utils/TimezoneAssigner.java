package utils;

import POJO.CityPojo;

import java.util.Map;

public class TimezoneAssigner {


    public static void assignTimezone(Map<String, CityPojo> cityPojoMap) {

        for(Map.Entry<String,CityPojo> entry:cityPojoMap.entrySet()){

            CityPojo currentCity = entry.getValue();
            String timezone = TimezoneRetriever.retrieveTimezone(currentCity.getLat(),currentCity.getLon());
            currentCity.setTimezone(timezone);
        }


    }
}
