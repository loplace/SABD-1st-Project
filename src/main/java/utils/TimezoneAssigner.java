package utils;

import POJO.CityPojo;
import net.xdevelop.jpclient.PyServeException;

import java.util.Map;

public class TimezoneAssigner {


    public static void assignTimezone(Map<String, CityPojo> cityPojoMap) throws PyServeException {

        for(Map.Entry<String,CityPojo> entry:cityPojoMap.entrySet()){

            CityPojo currentCity = entry.getValue();
            //String timezone = TimezoneRetriever.retrieveTimezone(currentCity.getLat(),currentCity.getLon());
            String timezone = "Europe/Rome";
            currentCity.setTimezone(timezone);
        }


    }
}
