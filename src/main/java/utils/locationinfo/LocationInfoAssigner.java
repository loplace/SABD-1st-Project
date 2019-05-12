package utils.locationinfo;

import model.CityModel;

import java.util.Map;

public class LocationInfoAssigner {


    public static void locationInfoAssign(Map<String, CityModel> cityPojoMap) {

        for(Map.Entry<String, CityModel> entry:cityPojoMap.entrySet()){

            CityModel currentCity = entry.getValue();
            String locationInfo = CityLocationRetriever.retrieveLocationInfo(currentCity.getLatitude(),currentCity.getLongitude());
            String[] tokens = locationInfo.split(";");
            String timezone = tokens[0];
            String country = tokens[1];
            currentCity.setTimezone(timezone);
            currentCity.setCountry(country);
        }


    }
}
