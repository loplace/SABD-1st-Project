package model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class CityModel implements Serializable, CityKey {

    String city;
    String timezone;
    String country;
    double latitude;
    double longitude;

    public CityModel(String city, double latitude, double longitude) {
        this.city = city;
        this.latitude = latitude;
        this.longitude = longitude;
    }

}
