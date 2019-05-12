package kMeans;

import java.io.Serializable;

public class CityModel implements Serializable {

    private String city;
    private String latitude;
    private String longitude;

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }




    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }


    public CityModel(String city, String lat, String longi) {
        this.city = city;
        this.latitude = lat;
        this.longitude = longi;
    }


    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }



}
