package POJO;

import java.io.Serializable;

public class CityPojo implements Serializable {

    String city;
    String timezone;
    String country;
    double lat;
    double lon;

    public CityPojo(String city, double lat, double lon) {
        this.city = city;
        this.lat = lat;
        this.lon = lon;
    }


    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCity() {
        return city;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    @Override
    public String toString() {
        return "CityPojo{" +
                "city='" + city + '\'' +
                ", timezone='" + timezone + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                '}';
    }
}
